package websocket

import (
	"encoding/json"
	"time"

	"github.com/Tencent/AI-Infra-Guard/internal/gologger"
	"github.com/Tencent/AI-Infra-Guard/pkg/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"gorm.io/datatypes"
)

const (
	// WebSocket相关常量
	maxMessageSize        = 512 * 1024 // 512KB
	pongWait              = 60 * time.Second
	writeWait             = 10 * time.Second
	WSMsgTypeRegister     = "register"
	WSMsgTypeHeartbeat    = "heartbeat"
	WSMsgTypeTaskProgress = "task_progress"
	WSMsgTypeTaskResult   = "task_result"
)

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

// 注册响应结构体（如需）
type AgentRegisterResponse struct {
	Type    string `json:"type"`
	Content struct {
		Status  int    `json:"status"`
		Message string `json:"message"`
	} `json:"content"`
}

var agentID string

// HandleAgentWebSocket 处理agent的WebSocket连接
func HandleAgentWebSocket(agentStore *database.AgentStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 升级HTTP连接为WebSocket连接
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		defer func() {
			if agentID != "" {
				agentStore.UpdateOnlineStatus(agentID, false)
			}
			conn.Close()
		}()

		// 设置连接参数
		conn.SetReadLimit(maxMessageSize)
		conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})

		// 处理消息
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var wsMsg WSMessage
			if err := json.Unmarshal(message, &wsMsg); err != nil {
				sendError(conn, "消息格式错误")
				continue
			}
			switch wsMsg.Type {
			case WSMsgTypeRegister:
				contentBytes, _ := json.Marshal(wsMsg.Content)
				var reg AgentRegisterContent
				json.Unmarshal(contentBytes, &reg)
				capabilitiesJSON, _ := json.Marshal(reg.Capabilities)
				agent := &database.Agent{
					ID:           reg.AgentID,
					Hostname:     reg.Hostname,
					IP:           reg.IP,
					Version:      reg.Version,
					Capabilities: datatypes.JSON(capabilitiesJSON),
					Meta:         reg.Meta,
					Online:       true,
				}
				if err := agentStore.Register(agent); err != nil {
					sendError(conn, "注册失败"+err.Error())
					continue
				}
				agentID = reg.AgentID
				response := WSMessage{
					Type: "register_ack",
					Content: Response{
						Status:  0,
						Message: "注册成功",
						Data:    nil,
					},
				}
				if err := conn.WriteJSON(response); err != nil {
					gologger.Errorf("发送注册响应失败: %v", err)
				}
			case WSMsgTypeHeartbeat:
				contentBytes, _ := json.Marshal(wsMsg.Content)
				var hb AgentRegisterContent
				json.Unmarshal(contentBytes, &hb)
				agentStore.UpdateLastSeen(hb.AgentID)
				agentStore.UpdateOnlineStatus(hb.AgentID, true)

			case WSMsgTypeTaskProgress:
				contentBytes, _ := json.Marshal(wsMsg.Content)
				var prog TaskProgressContent
				json.Unmarshal(contentBytes, &prog)
				// ...进度逻辑...
			case WSMsgTypeTaskResult:
				contentBytes, _ := json.Marshal(wsMsg.Content)
				var res TaskResultContent
				json.Unmarshal(contentBytes, &res)
				// ...结果逻辑...
			}
		}
	}
}

// sendError 发送错误响应
func sendError(conn *websocket.Conn, message string) {
	response := WSMessage{
		Type: "register_ack",
		Content: Response{
			Status:  1,
			Message: message,
			Data:    nil,
		},
	}
	conn.WriteJSON(response)
}
