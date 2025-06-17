package websocket

import (
	"encoding/json"
	"sync"
	"time"

	// "github.com/Tencent/AI-Infra-Guard/pkg/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	// "gorm.io/datatypes"
)

const (
	// WebSocket相关常量
	maxMessageSize        = 512 * 1024 // 512KB
	pongWait              = 60 * time.Second
	pingPeriod            = (pongWait * 9) / 10
	writeWait             = 10 * time.Second
	WSMsgTypeRegister     = "register"
	WSMsgTypeHeartbeat    = "heartbeat"
	WSMsgTypeTaskProgress = "task_progress"
	WSMsgTypeTaskResult   = "task_result"
	WSMsgTypeDisconnect   = "disconnect" // 新增：主动断开连接的消息类型
)

// AgentConnection 管理单个agent的连接
type AgentConnection struct {
	conn    *websocket.Conn
	agentID string
	// store    *database.AgentStore // 注释掉数据库字段
	mu       sync.Mutex
	isActive bool
}

// AgentManager 管理所有agent连接
type AgentManager struct {
	connections map[string]*AgentConnection
	mu          sync.RWMutex
	// store       *database.AgentStore // 注释掉数据库字段
}

// 注册/心跳消息内容
type AgentRegisterContent struct {
	AgentID      string   `json:"agent_id"`
	Hostname     string   `json:"hostname,omitempty"`
	IP           string   `json:"ip,omitempty"`
	Version      string   `json:"version,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
	Meta         string   `json:"meta,omitempty"`
}

// 任务进度消息内容
type TaskProgressContent struct {
	AgentID  string `json:"agent_id"`
	TaskID   string `json:"task_id"`
	Progress int    `json:"progress"`
	Log      string `json:"log"`
}

// 任务结果消息内容
type TaskResultContent struct {
	AgentID string      `json:"agent_id"`
	TaskID  string      `json:"task_id"`
	Status  string      `json:"status"`
	Result  interface{} `json:"result"`
}

// 断开连接消息内容
type DisconnectContent struct {
	AgentID string `json:"agent_id"`
	Reason  string `json:"reason,omitempty"`
}

// NewAgentManager 创建新的AgentManager
// func NewAgentManager(store *database.AgentStore) *AgentManager {
func NewAgentManager() *AgentManager {
	return &AgentManager{
		connections: make(map[string]*AgentConnection),
		// store:       store,
	}
}

// NewAgentConnection 创建新的AgentConnection
// func NewAgentConnection(conn *websocket.Conn, store *database.AgentStore) *AgentConnection {
func NewAgentConnection(conn *websocket.Conn) *AgentConnection {
	return &AgentConnection{
		conn: conn,
		// store:    store,
		isActive: true,
	}
}

// HandleAgentWebSocket 处理agent的WebSocket连接
func (am *AgentManager) HandleAgentWebSocket() gin.HandlerFunc {
	return func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}

		// ac := NewAgentConnection(conn, am.store)
		ac := NewAgentConnection(conn)
		go ac.handleConnection(am)
	}
}

// handleConnection 处理单个连接的消息
func (ac *AgentConnection) handleConnection(am *AgentManager) {
	defer func() {
		ac.cleanup(am)
	}()

	// 设置连接参数
	ac.conn.SetReadLimit(maxMessageSize)
	ac.conn.SetReadDeadline(time.Now().Add(pongWait))
	ac.conn.SetPongHandler(func(string) error {
		ac.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// 启动心跳检测
	go ac.writePump()

	// 处理消息
	for {
		_, message, err := ac.conn.ReadMessage()
		if err != nil {
			break
		}

		var wsMsg WSMessage
		if err := json.Unmarshal(message, &wsMsg); err != nil {
			ac.sendError("消息格式错误")
			continue
		}

		switch wsMsg.Type {
		case WSMsgTypeRegister:
			ac.handleRegister(am, wsMsg.Content)
		case WSMsgTypeHeartbeat:
			ac.handleHeartbeat(wsMsg.Content)
		case WSMsgTypeDisconnect:
			ac.handleDisconnect(am, wsMsg.Content)
			return
		case WSMsgTypeTaskProgress:
			ac.handleTaskProgress(wsMsg.Content)
		case WSMsgTypeTaskResult:
			ac.handleTaskResult(wsMsg.Content)
		}
	}
}

// handleRegister 处理注册消息
func (ac *AgentConnection) handleRegister(am *AgentManager, content interface{}) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	contentBytes, _ := json.Marshal(content)
	var reg AgentRegisterContent
	if err := json.Unmarshal(contentBytes, &reg); err != nil {
		ac.sendError("注册消息格式错误")
		return
	}

	// 检查是否已存在同ID的连接
	am.mu.Lock()
	if existingConn, exists := am.connections[reg.AgentID]; exists {
		// 关闭旧连接
		existingConn.isActive = false
		existingConn.conn.Close()
		delete(am.connections, reg.AgentID)
	}
	am.mu.Unlock()

	// capabilitiesJSON, _ := json.Marshal(reg.Capabilities)
	// agent := &database.Agent{
	// 	ID:           reg.AgentID,
	// 	Hostname:     reg.Hostname,
	// 	IP:           reg.IP,
	// 	Version:      reg.Version,
	// 	Capabilities: datatypes.JSON(capabilitiesJSON),
	// 	Meta:         reg.Meta,
	// 	Online:       true,
	// }

	// if err := ac.store.Register(agent); err != nil {
	// 	ac.sendError("注册失败: " + err.Error())
	// 	return
	// }

	ac.agentID = reg.AgentID
	am.mu.Lock()
	am.connections[reg.AgentID] = ac
	am.mu.Unlock()

	response := WSMessage{
		Type: "register_ack",
		Content: Response{
			Status:  0,
			Message: "注册成功",
		},
	}
	ac.conn.WriteJSON(response)
}

// handleHeartbeat 处理心跳消息
func (ac *AgentConnection) handleHeartbeat(content interface{}) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	contentBytes, _ := json.Marshal(content)
	var hb AgentRegisterContent
	if err := json.Unmarshal(contentBytes, &hb); err != nil {
		ac.sendError("心跳消息格式错误")
		return
	}

	// 验证身份一致性
	if ac.agentID == "" || ac.agentID != hb.AgentID {
		ac.sendError("心跳消息身份验证失败")
		return
	}

	// ac.store.UpdateLastSeen(hb.AgentID)
	// ac.store.UpdateOnlineStatus(hb.AgentID, true)
}

// handleDisconnect 处理主动断开连接
func (ac *AgentConnection) handleDisconnect(am *AgentManager, content interface{}) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	contentBytes, _ := json.Marshal(content)
	var dc DisconnectContent
	if err := json.Unmarshal(contentBytes, &dc); err != nil {
		ac.sendError("断开连接消息格式错误")
		return
	}

	// 验证身份一致性
	if ac.agentID == "" || ac.agentID != dc.AgentID {
		ac.sendError("断开连接消息身份验证失败")
		return
	}

	// ac.store.UpdateOnlineStatus(ac.agentID, false)

	// 从连接管理器中移除
	am.mu.Lock()
	delete(am.connections, ac.agentID)
	am.mu.Unlock()

	// 发送断开确认
	response := WSMessage{
		Type: "disconnect_ack",
		Content: Response{
			Status:  0,
			Message: "断开连接成功",
		},
	}
	ac.conn.WriteJSON(response)

	// 标记连接为非活跃
	ac.isActive = false
}

// writePump 发送心跳包
func (ac *AgentConnection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			ac.mu.Lock()
			if !ac.isActive {
				ac.mu.Unlock()
				return
			}
			ac.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := ac.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				ac.mu.Unlock()
				return
			}
			ac.mu.Unlock()
		}
	}
}

// cleanup 清理连接
func (ac *AgentConnection) cleanup(am *AgentManager) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if ac.agentID != "" {
		am.mu.Lock()
		delete(am.connections, ac.agentID)
		am.mu.Unlock()

		// ac.store.UpdateOnlineStatus(ac.agentID, false)
	}

	ac.isActive = false
	ac.conn.Close()
}

// handleTaskProgress 处理任务进度消息
func (ac *AgentConnection) handleTaskProgress(content interface{}) {
	contentBytes, _ := json.Marshal(content)
	var prog TaskProgressContent
	json.Unmarshal(contentBytes, &prog)
	// ...进度逻辑...
}

// handleTaskResult 处理任务结果消息
func (ac *AgentConnection) handleTaskResult(content interface{}) {
	contentBytes, _ := json.Marshal(content)
	var res TaskResultContent
	json.Unmarshal(contentBytes, &res)
	// ...结果逻辑...
}

// sendError 发送错误响应
func (ac *AgentConnection) sendError(message string) {
	response := WSMessage{
		Type: "error",
		Content: Response{
			Status:  1,
			Message: message,
		},
	}
	ac.conn.WriteJSON(response)
}
