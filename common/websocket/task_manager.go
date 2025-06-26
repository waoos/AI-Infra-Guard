package websocket

import (
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/Tencent/AI-Infra-Guard/internal/gologger"
	"github.com/Tencent/AI-Infra-Guard/pkg/database"
	"gorm.io/datatypes"
)

// 任务管理器相关数据结构

const (
	WSMsgTypeTaskAssign = "task_assign" // 任务分配
)

type TaskManager struct {
	mu           sync.RWMutex
	tasks        map[string]*TaskCreateRequest // sessionId -> 任务请求
	agentManager *AgentManager                 // 新增：引用 AgentManager
	taskStore    *database.TaskStore           // 新增：引用 TaskStore
	fileConfig   *FileUploadConfig             // 新增：文件上传配置
	sseManager   *SSEManager                   // 新增：SSE管理器
}

func NewTaskManager(agentManager *AgentManager, taskStore *database.TaskStore, fileConfig *FileUploadConfig, sseManager *SSEManager) *TaskManager {
	if fileConfig == nil {
		fileConfig = DefaultFileUploadConfig()
	}
	if sseManager == nil {
		sseManager = NewSSEManager()
	}
	return &TaskManager{
		tasks:        make(map[string]*TaskCreateRequest),
		agentManager: agentManager, // 注入 AgentManager
		taskStore:    taskStore,    // 注入 TaskStore
		fileConfig:   fileConfig,   // 注入文件上传配置
		sseManager:   sseManager,   // 注入SSE管理器
	}
}

// 添加任务
func (tm *TaskManager) AddTask(req *TaskCreateRequest) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 存储任务到内存
	tm.tasks[req.SessionID] = req

	// 在数据库中创建会话记录
	session := &database.Session{
		ID:            req.SessionID,
		Username:      req.Username,
		Title:         generateTitle(req.Content),
		TaskType:      req.Task,
		Content:       req.Content,
		Params:        mustMarshalJSON(req.Params),
		Attachments:   mustMarshalJSON(req.Attachments),
		Status:        "doing",
		AssignedAgent: "",
	}

	err := tm.taskStore.CreateSession(session)
	if err != nil {
		return fmt.Errorf("创建会话记录失败: %v", err)
	}

	// 异步分发任务
	go tm.dispatchTask(req.SessionID)

	return nil
}

// 获取任务
func (tm *TaskManager) GetTask(sessionId string) (*TaskCreateRequest, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	task, ok := tm.tasks[sessionId]
	return task, ok
}

// 新增：任务分发方法
func (tm *TaskManager) dispatchTask(sessionId string) {
	// 1. 获取任务
	task, exists := tm.GetTask(sessionId)
	if !exists {
		gologger.Errorf("Task not found for sessionId: %s", sessionId)
		return
	}

	// 2. 获取可用 Agent
	availableAgents := tm.agentManager.GetAvailableAgents()
	if len(availableAgents) == 0 {
		gologger.Info("No available agents")
		return
	}

	// 3. 选择 Agent（简单策略）
	selectedAgent := availableAgents[0]

	// 4. 更新session的assigned_agent和开始时间
	err := tm.taskStore.UpdateSessionAssignedAgent(task.SessionID, selectedAgent.agentID)
	if err != nil {
		gologger.Errorf("Failed to update session assigned agent: %v", err)
		return
	}

	// 5. 构造任务分配消息
	taskMsg := WSMessage{
		Type: WSMsgTypeTaskAssign,
		Content: TaskContent{
			SessionID:   task.SessionID,
			TaskType:    task.Task,
			Content:     task.Content,
			Params:      task.Params,
			Attachments: task.Attachments,
			Timeout:     3600,
		},
	}

	// 6. 发送给 Agent
	err = selectedAgent.conn.WriteJSON(taskMsg)
	if err != nil {
		gologger.Errorf("Failed to send task to agent %s: %v", selectedAgent.agentID, err)
		// 如果发送失败，可以重置assigned_agent
		tm.taskStore.UpdateSessionAssignedAgent(task.SessionID, "")
		return
	}

	gologger.Infof("Task %s assigned to agent %s", task.SessionID, selectedAgent.agentID)
}

// HandleAgentEvent 处理来自Agent的事件
func (tm *TaskManager) HandleAgentEvent(sessionId string, eventType string, event interface{}) {
	gologger.Debugf("收到Agent事件: sessionId=%s, eventType=%s", sessionId, eventType)

	// 直接使用通用事件处理函数
	tm.handleEvent(sessionId, eventType, event)

	// 根据事件类型记录特定日志
	switch eventType {
	case "liveStatus":
		if liveStatusEvent, ok := event.(LiveStatusEvent); ok {
			gologger.Debugf("liveStatus事件详情: sessionId=%s, text=%s", sessionId, liveStatusEvent.Text)
		}
	case "planUpdate":
		if planUpdateEvent, ok := event.(PlanUpdateEvent); ok {
			gologger.Debugf("planUpdate事件详情: sessionId=%s, tasks=%d", sessionId, len(planUpdateEvent.Tasks))
		}
	case "newPlanStep":
		if newPlanStepEvent, ok := event.(NewPlanStepEvent); ok {
			gologger.Debugf("newPlanStep事件详情: sessionId=%s, stepId=%s", sessionId, newPlanStepEvent.StepID)
		}
	case "statusUpdate":
		if statusUpdateEvent, ok := event.(StatusUpdateEvent); ok {
			gologger.Debugf("statusUpdate事件详情: sessionId=%s, status=%s", sessionId, statusUpdateEvent.AgentStatus)
		}
	case "toolUsed":
		if toolUsedEvent, ok := event.(ToolUsedEvent); ok {
			gologger.Debugf("toolUsed事件详情: sessionId=%s, tools=%d", sessionId, len(toolUsedEvent.Tools))
		}
	case "actionLog":
		if actionLogEvent, ok := event.(ActionLogEvent); ok {
			gologger.Debugf("actionLog事件详情: sessionId=%s, actionId=%s", sessionId, actionLogEvent.ActionID)
		}
	default:
		gologger.Warnf("未知的事件类型: %s", eventType)
	}
}

// generateSecureFileName 生成安全的唯一文件名
func generateSecureFileName(originalName string) string {
	// 获取文件扩展名
	ext := filepath.Ext(originalName)

	// 生成UUID作为文件名前缀
	uuid := generateUUID()

	// 组合：UUID + 时间戳 + 扩展名
	timestamp := time.Now().Format("20060102150405")
	return fmt.Sprintf("%s_%s%s", uuid, timestamp, ext)
}

// generateUUID 生成简单的UUID（实际项目中建议使用标准UUID库）
func generateUUID() string {
	return fmt.Sprintf("%d_%d", time.Now().UnixNano(), time.Now().Unix())
}

// 通用事件处理函数
func (tm *TaskManager) handleEvent(sessionId string, eventType string, event interface{}) {
	// 生成事件ID
	id := generateEventID()

	// 获取事件的时间戳
	timestamp := getEventTimestamp(event)

	// 存储事件到数据库
	err := tm.taskStore.StoreEvent(id, sessionId, eventType, event, timestamp)
	if err != nil {
		gologger.Errorf("存储%s事件失败: %v", eventType, err)
		return
	}

	// 推送给前端 SSE
	err = tm.sseManager.SendEvent(id, sessionId, eventType, event)
	if err != nil {
		gologger.Errorf("推送%s事件到SSE失败: %v", eventType, err)
		return
	}

	// 记录日志
	gologger.Debugf("%s事件已处理: sessionId=%s", eventType, sessionId)
}

// getEventTimestamp 获取事件的时间戳
func getEventTimestamp(event interface{}) int64 {
	// 使用反射获取Timestamp字段
	v := reflect.ValueOf(event)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() == reflect.Struct {
		if field := v.FieldByName("Timestamp"); field.IsValid() && field.CanInterface() {
			if timestamp, ok := field.Interface().(int64); ok {
				return timestamp
			}
		}
	}

	// 如果无法获取时间戳，使用当前时间
	return time.Now().UnixMilli()
}

// TerminateTask 终止任务
func (tm *TaskManager) TerminateTask(sessionId string, username string) error {
	// 检查任务是否存在
	session, err := tm.taskStore.GetSession(sessionId)
	if err != nil {
		return fmt.Errorf("任务不存在")
	}

	// 验证用户权限（只有任务创建者才能终止任务）
	if session.Username != username {
		return fmt.Errorf("无权限操作此任务")
	}

	// 通知 Agent 终止任务
	if session.AssignedAgent != "" {
		tm.notifyAgentToTerminate(session.AssignedAgent, sessionId)
	}

	// 更新任务状态
	err = tm.taskStore.UpdateSessionStatus(sessionId, "terminated")
	if err != nil {
		return fmt.Errorf("更新任务状态失败")
	}

	// 发送终止事件给前端
	tm.sendTerminationEvent(sessionId)

	return nil
}

// notifyAgentToTerminate 通知 Agent 终止任务
func (tm *TaskManager) notifyAgentToTerminate(agentID string, sessionId string) {
	// 获取 Agent 连接
	availableAgents := tm.agentManager.GetAvailableAgents()
	for _, agent := range availableAgents {
		if agent.agentID == agentID {
			// 发送终止消息给 Agent
			terminateMsg := WSMessage{
				Type: "terminate",
				Content: map[string]interface{}{
					"session_id": sessionId,
					"reason":     "用户主动终止",
				},
			}
			agent.conn.WriteJSON(terminateMsg)
			break
		}
	}
}

// sendTerminationEvent 发送终止事件给前端
func (tm *TaskManager) sendTerminationEvent(sessionId string) {
	event := StatusUpdateEvent{
		ID:          generateEventID(),
		Type:        "statusUpdate",
		Timestamp:   time.Now().UnixMilli(),
		AgentStatus: "terminated",
		Brief:       "任务已终止",
		Description: "用户主动终止了任务执行",
		NoRender:    false,
	}

	// 使用通用事件处理函数
	tm.handleEvent(sessionId, "statusUpdate", event)

	gologger.Infof("终止事件已发送: sessionId=%s", sessionId)
}

// generateEventID 生成事件ID
func generateEventID() string {
	return time.Now().Format("20060102150405") + "_" + fmt.Sprintf("%d", time.Now().UnixNano())
}

// UpdateTask 更新任务信息
func (tm *TaskManager) UpdateTask(sessionId string, req *TaskUpdateRequest, username string) error {
	// 检查任务是否存在
	session, err := tm.taskStore.GetSession(sessionId)
	if err != nil {
		return fmt.Errorf("任务不存在")
	}

	// 验证用户权限（只有任务创建者才能更新任务）
	if session.Username != username {
		return fmt.Errorf("无权限操作此任务")
	}

	// 更新任务信息
	updates := map[string]interface{}{}

	if req.Title != "" {
		updates["title"] = req.Title
	}

	// 执行数据库更新
	err = tm.taskStore.UpdateSession(sessionId, updates)
	if err != nil {
		return fmt.Errorf("更新任务信息失败: %v", err)
	}

	return nil
}

// DeleteTask 删除任务
func (tm *TaskManager) DeleteTask(sessionId string, username string) error {
	// 检查任务是否存在
	session, err := tm.taskStore.GetSession(sessionId)
	if err != nil {
		return fmt.Errorf("任务不存在")
	}

	// 验证用户权限（只有任务创建者才能删除任务）
	if session.Username != username {
		return fmt.Errorf("无权限操作此任务")
	}

	// 删除相关的消息记录
	// 数据库操作：删除该会话下的所有 TaskMessage 记录
	// err = tm.taskStore.DeleteSessionMessages(sessionId)
	// if err != nil {
	//     return fmt.Errorf("删除任务消息失败: %v", err)
	// }

	// 删除会话记录
	err = tm.taskStore.DeleteSession(sessionId)
	if err != nil {
		return fmt.Errorf("删除任务失败: %v", err)
	}

	// 清理内存中的任务数据
	tm.mu.Lock()
	delete(tm.tasks, sessionId)
	tm.mu.Unlock()

	return nil
}

// UploadFileResult 文件上传结果
type UploadFileResult struct {
	OriginalName string `json:"original_name"` // 原始文件名
	FileURL      string `json:"file_url"`      // 文件访问URL
}

// UploadFile 上传文件
func (tm *TaskManager) UploadFile(file *multipart.FileHeader) (*UploadFileResult, error) {
	// 保存原始文件名
	originalName := file.Filename

	// 生成安全的唯一文件名
	fileName := generateSecureFileName(file.Filename)

	// 使用配置的上传目录
	uploadDir := tm.fileConfig.UploadDir
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return nil, fmt.Errorf("创建上传目录失败: %v", err)
	}

	// 创建文件路径
	filePath := filepath.Join(uploadDir, fileName)

	// 保存文件到本地
	src, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %v", err)
	}
	defer src.Close()

	dst, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("创建文件失败: %v", err)
	}
	defer dst.Close()

	// 复制文件内容并验证
	written, err := io.Copy(dst, src)
	if err != nil {
		// 清理已创建的文件
		os.Remove(filePath)
		return nil, fmt.Errorf("保存文件失败: %v", err)
	}

	// 验证写入的文件大小
	if written != file.Size {
		os.Remove(filePath)
		return nil, fmt.Errorf("文件写入不完整")
	}

	// 生成文件访问URL
	fileURL := tm.fileConfig.GetFileURL(fileName)

	return &UploadFileResult{
		OriginalName: originalName,
		FileURL:      fileURL,
	}, nil
}

// GetUserTasks 获取指定用户的任务列表，只返回属于该用户的会话，确保用户只能看到自己的任务。
func (tm *TaskManager) GetUserTasks(username string) ([]map[string]interface{}, error) {
	// 从数据库获取用户的所有会话（已做严格过滤）
	sessions, err := tm.taskStore.GetUserSessions(username)
	if err != nil {
		return nil, fmt.Errorf("获取用户任务失败: %v", err)
	}

	// 转换为前端需要的格式
	var tasks []map[string]interface{}
	for _, session := range sessions {
		task := map[string]interface{}{
			"sessionId": session.ID,
			"title":     session.Title,
			"taskType":  session.TaskType,
			"status":    session.Status,
			"updatedAt": session.UpdatedAt, // 直接使用时间戳毫秒级
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// 辅助函数：生成任务标题
func generateTitle(content string) string {
	// 使用rune来正确处理UTF-8字符，避免截断中文字符
	runes := []rune(content)
	if len(runes) > 50 {
		return string(runes[:50])
	}
	return content
}

// 辅助函数：将interface{}转换为datatypes.JSON
func mustMarshalJSON(v interface{}) datatypes.JSON {
	if v == nil {
		return datatypes.JSON("{}")
	}
	data, err := json.Marshal(v)
	if err != nil {
		return datatypes.JSON("{}")
	}
	return datatypes.JSON(data)
}

// EstablishSSEConnection 建立SSE连接
func (tm *TaskManager) EstablishSSEConnection(w http.ResponseWriter, sessionId string, username string) error {
	return tm.sseManager.AddConnection(sessionId, username, w)
}

// CloseSSESession 关闭SSE会话
func (tm *TaskManager) CloseSSESession(sessionId string) {
	tm.sseManager.RemoveConnection(sessionId)
}

// 任务完成时的清理
func (tm *TaskManager) cleanupTask(sessionId string) {
	// 清理内存中的任务数据
	tm.mu.Lock()
	delete(tm.tasks, sessionId)
	tm.mu.Unlock()

	// 关闭SSE连接
	tm.CloseSSESession(sessionId)

	gologger.Infof("任务清理完成: sessionId=%s", sessionId)
}
