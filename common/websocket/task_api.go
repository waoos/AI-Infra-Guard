package websocket

import (
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// 参数校验函数

// isValidSessionID 验证会话ID格式
func isValidSessionID(sessionId string) bool {
	// 只允许字母、数字、下划线、连字符
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9_-]+$`, sessionId)
	return matched && len(sessionId) > 0 && len(sessionId) <= 50
}

// validateFileUpload 验证文件上传
func validateFileUpload(header *multipart.FileHeader) error {
	// 1. 文件名安全验证
	originalName := header.Filename
	if originalName == "" {
		return fmt.Errorf("文件名不能为空")
	}

	// 防止路径遍历攻击
	if strings.Contains(originalName, "..") || strings.Contains(originalName, "/") || strings.Contains(originalName, "\\") {
		return fmt.Errorf("文件名包含非法字符")
	}

	// 2. 文件扩展名验证
	ext := strings.ToLower(filepath.Ext(originalName))
	allowedExtensions := map[string]bool{
		".txt":  true,
		".pdf":  true,
		".doc":  true,
		".docx": true,
		".jpg":  true,
		".jpeg": true,
		".png":  true,
		".gif":  true,
		".zip":  true,
		".rar":  true,
	}
	if !allowedExtensions[ext] {
		return fmt.Errorf("不支持的文件类型: %s", ext)
	}

	// 3. 文件内容类型验证
	contentType := header.Header.Get("Content-Type")
	allowedMimeTypes := map[string]bool{
		"text/plain":         true,
		"application/pdf":    true,
		"application/msword": true,
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document": true,
		"image/jpeg":                   true,
		"image/png":                    true,
		"image/gif":                    true,
		"application/zip":              true,
		"application/x-rar-compressed": true,
	}
	if !allowedMimeTypes[contentType] {
		return fmt.Errorf("不支持的文件内容类型: %s", contentType)
	}

	return nil
}

// validateTaskUpdateRequest 验证任务更新请求
func validateTaskUpdateRequest(req *TaskUpdateRequest) error {
	if req.Title != "" {
		// 清理和验证标题
		req.Title = strings.TrimSpace(req.Title)
		if len(req.Title) > 100 {
			return fmt.Errorf("标题长度超过限制")
		}
	}
	return nil
}

// 预留任务相关接口实现

// SSE接口（心跳推送）
func HandleTaskSSE(c *gin.Context, tm *TaskManager) {
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Flush()

	sessionId := c.Query("sessionId")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case t := <-ticker.C:
			msg := TaskEventMessage{
				ID:        fmt.Sprintf("%d", t.UnixNano()),
				Type:      "event",
				SessionID: sessionId,
				Timestamp: t.UnixMilli(),
				Event: LiveStatusEvent{
					ID:        fmt.Sprintf("%d", t.UnixNano()),
					Type:      "liveStatus",
					Timestamp: t.UnixMilli(),
					Text:      "存活中",
				},
			}
			// SSE格式：data: <json>\n\n
			jsonStr, _ := json.Marshal(msg)
			c.Writer.Write([]byte("data: "))
			c.Writer.Write(jsonStr)
			c.Writer.Write([]byte("\n\n"))
			c.Writer.Flush()
		}
	}
}

// 新建任务接口
func HandleTaskCreate(c *gin.Context, tm *TaskManager) {
	var req TaskCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "参数错误",
			"data":    nil,
		})
		return
	}

	// 从IOA中间件获取员工ID
	userID, exists := c.Get("user_id")
	if !exists || userID.(string) == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "用户未认证，请先登录",
			"data":    nil,
		})
		return
	}

	// 设置用户ID到请求中
	req.UserID = userID.(string)

	// 调用TaskManager
	err := tm.AddTask(&req)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "任务创建失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  0,
		"message": "任务创建成功",
		"data": gin.H{
			"sessionId": req.SessionID,
		},
	})
}

// 终止任务接口
func HandleTerminateTask(c *gin.Context, tm *TaskManager) {
	sessionId := c.Param("sessionId")
	if sessionId == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "会话ID不能为空",
			"data":    nil,
		})
		return
	}

	// 验证sessionId格式
	if !isValidSessionID(sessionId) {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "无效的会话ID格式",
			"data":    nil,
		})
		return
	}

	// 从IOA中间件获取员工ID
	userID, exists := c.Get("user_id")
	if !exists || userID.(string) == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "用户未认证，请先登录",
			"data":    nil,
		})
		return
	}

	// 调用TaskManager（包含权限验证）
	err := tm.TerminateTask(sessionId, userID.(string))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "任务终止失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  0,
		"message": "任务终止成功",
		"data":    nil,
	})
}

// 更新任务信息接口
func HandleUpdateTask(c *gin.Context, tm *TaskManager) {
	sessionId := c.Param("sessionId")
	if sessionId == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "会话ID不能为空",
			"data":    nil,
		})
		return
	}

	// 验证sessionId格式
	if !isValidSessionID(sessionId) {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "无效的会话ID格式",
			"data":    nil,
		})
		return
	}

	var req TaskUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "参数错误: " + err.Error(),
			"data":    nil,
		})
		return
	}

	// 验证任务更新请求
	if err := validateTaskUpdateRequest(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "请求参数验证失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	// 从IOA中间件获取员工ID
	userID, exists := c.Get("user_id")
	if !exists || userID.(string) == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "用户未认证，请先登录",
			"data":    nil,
		})
		return
	}

	// 执行任务信息更新（包含权限验证）
	err := tm.UpdateTask(sessionId, &req, userID.(string))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "任务信息更新失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  0,
		"message": "任务信息更新成功",
		"data":    nil,
	})
}

// 删除任务接口
func HandleDeleteTask(c *gin.Context, tm *TaskManager) {
	sessionId := c.Param("sessionId")
	if sessionId == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "会话ID不能为空",
			"data":    nil,
		})
		return
	}

	// 验证sessionId格式
	if !isValidSessionID(sessionId) {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "无效的会话ID格式",
			"data":    nil,
		})
		return
	}

	// 从IOA中间件获取员工ID
	userID, exists := c.Get("user_id")
	if !exists || userID.(string) == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "用户未认证，请先登录",
			"data":    nil,
		})
		return
	}

	// 执行任务删除（包含权限验证）
	err := tm.DeleteTask(sessionId, userID.(string))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "任务删除失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  0,
		"message": "任务删除成功",
		"data":    nil,
	})
}

// 文件上传接口
func HandleUploadFile(c *gin.Context, tm *TaskManager) {
	// 从IOA中间件获取员工ID
	userID, exists := c.Get("user_id")
	if !exists || userID.(string) == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "用户未认证，请先登录",
			"data":    nil,
		})
		return
	}

	// 获取上传的文件
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "获取上传文件失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	// 验证文件
	if err := validateFileUpload(file); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "文件验证失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	// 执行文件上传
	fileUrl, err := tm.UploadFile(file)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "文件上传失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  0,
		"message": "文件上传成功",
		"data": gin.H{
			"fileUrl": fileUrl,
		},
	})
}

// 获取任务列表接口
func HandleGetTaskList(c *gin.Context, tm *TaskManager) {
	// 从IOA中间件获取员工ID
	userID, exists := c.Get("user_id")
	if !exists || userID.(string) == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "用户未认证，请先登录",
			"data":    nil,
		})
		return
	}

	// 获取用户的任务列表
	tasks, err := tm.GetUserTasks(userID.(string))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  1,
			"message": "获取任务列表失败: " + err.Error(),
			"data":    nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  0,
		"message": "获取任务列表成功",
		"data": gin.H{
			"tasks": tasks,
		},
	})
}
