package websocket

import (
	"encoding/json"
	"time"

	"github.com/Tencent/AI-Infra-Guard/pkg/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"gorm.io/datatypes"
)

const (
	// WebSocket相关常量
	maxMessageSize = 512 * 1024 // 512KB
	pongWait       = 60 * time.Second
	writeWait      = 10 * time.Second
)

var agentID string

// AgentRegisterRequest 表示agent注册请求
type AgentRegisterRequest struct {
	Type    string `json:"type"`
	Content struct {
		AgentID      string   `json:"agent_id"`
		Hostname     string   `json:"hostname"`
		IP           string   `json:"ip"`
		Version      string   `json:"version"`
		Capabilities []string `json:"capabilities"`
		Meta         string   `json:"meta"`
	} `json:"content"`
}

// AgentRegisterResponse 表示agent注册响应
type AgentRegisterResponse struct {
	Type    string `json:"type"`
	Content struct {
		Status  int    `json:"status"`
		Message string `json:"message"`
	} `json:"content"`
}

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

			// 解析消息
			var req AgentRegisterRequest
			if err := json.Unmarshal(message, &req); err != nil {
				sendError(conn, "消息格式错误")
				continue
			}

			// 处理注册请求
			if req.Type == "register" {
				agentID = req.Content.AgentID
				// 创建agent记录
				capabilitiesJSON, _ := json.Marshal(req.Content.Capabilities)
				agent := &database.Agent{
					ID:           req.Content.AgentID,
					Hostname:     req.Content.Hostname,
					IP:           req.Content.IP,
					Version:      req.Content.Version,
					Capabilities: datatypes.JSON(capabilitiesJSON),
					Meta:         req.Content.Meta,
					Online:       true,
				}

				// 保存到数据库
				if err := agentStore.Register(agent); err != nil {
					sendError(conn, "注册失败: "+err.Error())
					continue
				}

				// 发送成功响应
				response := AgentRegisterResponse{
					Type: "register_ack",
				}
				response.Content.Status = 0
				response.Content.Message = "注册成功"

				if err := conn.WriteJSON(response); err != nil {
					break
				}
			}

			// 处理心跳消息
			if req.Type == "heartbeat" {
				agentStore.UpdateLastSeen(req.Content.AgentID)
				agentStore.UpdateOnlineStatus(req.Content.AgentID, true)
				// 可选：回复心跳ack
				continue
			}
		}
	}
}

// sendError 发送错误响应
func sendError(conn *websocket.Conn, message string) {
	response := AgentRegisterResponse{
		Type: "register_ack",
	}
	response.Content.Status = 1
	response.Content.Message = message
	conn.WriteJSON(response)
}
