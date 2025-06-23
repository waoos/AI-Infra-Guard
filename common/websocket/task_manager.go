package websocket

import (
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
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
}

func NewTaskManager(agentManager *AgentManager, taskStore *database.TaskStore) *TaskManager {
	return &TaskManager{
		tasks:        make(map[string]*TaskCreateRequest),
		agentManager: agentManager, // 注入 AgentManager
		taskStore:    taskStore,    // 注入 TaskStore
	}
}

// 添加任务
func (tm *TaskManager) AddTask(req *TaskCreateRequest) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 存储任务
	tm.tasks[req.SessionID] = req

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

	// 4. 构造任务分配消息
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

	// 5. 发送给 Agent
	selectedAgent.conn.WriteJSON(taskMsg)
}

// 处理 Agent 返回的事件
func (tm *TaskManager) HandleAgentEvent(eventType string, event interface{}) {
	// 根据事件类型处理
	switch eventType {
	case "liveStatus":
		if liveEvent, ok := event.(LiveStatusEvent); ok {
			tm.handleLiveStatusEvent(liveEvent)
		}
	case "planUpdate":
		if planEvent, ok := event.(PlanUpdateEvent); ok {
			tm.handlePlanUpdateEvent(planEvent)
		}
	case "newPlanStep":
		if stepEvent, ok := event.(NewPlanStepEvent); ok {
			tm.handleNewPlanStepEvent(stepEvent)
		}
	case "statusUpdate":
		if statusEvent, ok := event.(StatusUpdateEvent); ok {
			tm.handleStatusUpdateEvent(statusEvent)
		}
	case "toolUsed":
		if toolEvent, ok := event.(ToolUsedEvent); ok {
			tm.handleToolUsedEvent(toolEvent)
		}
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

// 处理各种事件的具体方法
func (tm *TaskManager) handleLiveStatusEvent(event LiveStatusEvent) {
	// 这里可以添加事件存储逻辑
	// tm.storeEvent(event)

	// 推送给前端 SSE（需要实现 SSE 连接管理）
	// tm.pushEventToSSE(event)

	// 更新任务状态
	// tm.updateTaskStatus(event.SessionID, "running")
}

func (tm *TaskManager) handlePlanUpdateEvent(event PlanUpdateEvent) {
	// 处理计划更新事件
}

func (tm *TaskManager) handleNewPlanStepEvent(event NewPlanStepEvent) {
	// 处理新计划步骤事件
}

func (tm *TaskManager) handleStatusUpdateEvent(event StatusUpdateEvent) {
	// 处理状态更新事件
}

func (tm *TaskManager) handleToolUsedEvent(event ToolUsedEvent) {
	// 处理插件使用事件
}

// TerminateTask 终止任务
func (tm *TaskManager) TerminateTask(sessionId string, userID string) error {
	// 检查任务是否存在
	session, err := tm.taskStore.GetSession(sessionId)
	if err != nil {
		return fmt.Errorf("任务不存在")
	}

	// 验证用户权限（只有任务创建者才能终止任务）
	if session.UserID != userID {
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
		Timestamp:   time.Now().Unix(),
		AgentStatus: "terminated",
		Brief:       "任务已终止",
		Description: "用户主动终止了任务执行",
		NoRender:    false,
	}

	// 存储事件
	tm.taskStore.StoreEvent(sessionId, "statusUpdate", event, event.Timestamp)

	// 推送给前端 SSE（需要实现 SSE 推送逻辑）
	// tm.pushEventToSSE(sessionId, "statusUpdate", event)
}

// generateEventID 生成事件ID
func generateEventID() string {
	return time.Now().Format("20060102150405") + "_" + fmt.Sprintf("%d", time.Now().UnixNano())
}

// UpdateTask 更新任务信息
func (tm *TaskManager) UpdateTask(sessionId string, req *TaskUpdateRequest, userID string) error {
	// 检查任务是否存在
	session, err := tm.taskStore.GetSession(sessionId)
	if err != nil {
		return fmt.Errorf("任务不存在")
	}

	// 验证用户权限（只有任务创建者才能更新任务）
	if session.UserID != userID {
		return fmt.Errorf("无权限操作此任务")
	}

	// 更新任务信息
	updates := map[string]interface{}{
		"updated_at": time.Now(),
	}

	if req.Title != "" {
		updates["title"] = req.Title
	}

	// 执行数据库更新
	// err = tm.taskStore.UpdateSession(sessionId, updates)
	if err != nil {
		return fmt.Errorf("更新任务信息失败")
	}

	return nil
}

// DeleteTask 删除任务
func (tm *TaskManager) DeleteTask(sessionId string, userID string) error {
	// 检查任务是否存在
	session, err := tm.taskStore.GetSession(sessionId)
	if err != nil {
		return fmt.Errorf("任务不存在")
	}

	// 验证用户权限（只有任务创建者才能删除任务）
	if session.UserID != userID {
		return fmt.Errorf("无权限操作此任务")
	}

	// 删除相关的消息记录
	// 数据库操作：删除该会话下的所有 TaskMessage 记录
	// err = tm.taskStore.DeleteSessionMessages(sessionId)
	// if err != nil {
	//     return fmt.Errorf("删除任务消息失败: %v", err)
	// }

	// 删除会话记录
	// 数据库操作：删除 Session 记录
	// err = tm.taskStore.DeleteSession(sessionId)
	// if err != nil {
	//     return fmt.Errorf("删除任务失败: %v", err)
	// }

	// 清理内存中的任务数据
	tm.mu.Lock()
	delete(tm.tasks, sessionId)
	tm.mu.Unlock()

	return nil
}

// UploadFile 上传文件
func (tm *TaskManager) UploadFile(file *multipart.FileHeader) (string, error) {
	// 生成安全的唯一文件名
	fileName := generateSecureFileName(file.Filename)

	// 创建上传目录
	uploadDir := "./uploads"
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return "", fmt.Errorf("创建上传目录失败: %v", err)
	}

	// 创建文件路径
	filePath := filepath.Join(uploadDir, fileName)

	// 保存文件到本地
	src, err := file.Open()
	if err != nil {
		return "", fmt.Errorf("打开文件失败: %v", err)
	}
	defer src.Close()

	dst, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("创建文件失败: %v", err)
	}
	defer dst.Close()

	// 复制文件内容并验证
	written, err := io.Copy(dst, src)
	if err != nil {
		// 清理已创建的文件
		os.Remove(filePath)
		return "", fmt.Errorf("保存文件失败: %v", err)
	}

	// 验证写入的文件大小
	if written != file.Size {
		os.Remove(filePath)
		return "", fmt.Errorf("文件写入不完整")
	}

	// 返回文件URL
	fileUrl := fmt.Sprintf("/uploads/%s", fileName)

	return fileUrl, nil
}

// GetUserTasks 获取任务列表
func (tm *TaskManager) GetUserTasks(userID string) ([]map[string]interface{}, error) {
	// 从数据库获取用户的所有会话
	sessions, err := tm.taskStore.GetUserSessions(userID)
	if err != nil {
		return nil, fmt.Errorf("获取用户任务失败: %v", err)
	}

	// 转换为前端需要的格式
	var tasks []map[string]interface{}
	for _, session := range sessions {
		task := map[string]interface{}{
			"id":        session.ID,
			"title":     session.Title,
			"content":   session.Content,
			"status":    session.Status,
			"createdAt": session.CreatedAt.Format("2006-01-02T15:04:05Z"),
			"updatedAt": session.UpdatedAt.Format("2006-01-02T15:04:05Z"),
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// 辅助函数：生成任务标题
func generateTitle(content string) string {
	if len(content) > 50 {
		return content[:50]
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
