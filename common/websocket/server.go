package websocket

import (
	"embed"
	"mime"
	"path/filepath"

	"github.com/Tencent/AI-Infra-Guard/common/runner"
	"github.com/Tencent/AI-Infra-Guard/internal/gologger"
	"github.com/Tencent/AI-Infra-Guard/internal/options"
	"github.com/gin-gonic/gin"
)

//go:embed static/*
var staticFS embed.FS

func RunWebServer(options *options.Options) {
	r := gin.Default()
	wsServer := NewWSServer(options)

	// API 版本分组
	v1 := r.Group("/api/v1")
	{
		// 1. 知识库模块
		knowledge := v1.Group("/knowledge")
		{
			// 对抗样本库
			knowledge.Group("/samples")

			// AI应用指纹
			fingerprints := knowledge.Group("/fingerprints")
			{
				// 管理功能
				fingerprints.GET("", func(c *gin.Context) {
					gologger.Infoln("收到指纹列表请求")

					// 调用原有的指纹列表实现
					runner, err := runner.New(options)
					if err != nil {
						gologger.Errorf("创建runner失败: %v", err)
						c.JSON(500, gin.H{
							"status":  1,
							"message": err.Error(),
							"data":    nil,
						})
						return
					}
					defer runner.Close()

					data := runner.GetFpAndVulList()
					gologger.Infof("获取到指纹列表数据，长度: %d", len(data))

					response := gin.H{
						"status":  0,
						"message": "success",
						"data":    data,
					}
					gologger.Infof("返回数据: %+v", response)
					c.JSON(200, response)
				})
			}

			// 漏洞库
			vulnerabilities := knowledge.Group("/vulnerabilities")
			{
				// 管理功能
				vulnerabilities.GET("", func(c *gin.Context) {
					gologger.Infoln("收到漏洞列表请求")

					// 调用原有的漏洞列表实现
					runner, err := runner.New(options)
					if err != nil {
						gologger.Errorf("创建runner失败: %v", err)
						c.JSON(500, gin.H{
							"status":  1,
							"message": err.Error(),
							"data":    nil,
						})
						return
					}
					defer runner.Close()

					data := runner.GetFpAndVulList()
					gologger.Infof("获取到漏洞列表数据，长度: %d", len(data))

					response := gin.H{
						"status":  0,
						"message": "success",
						"data":    data,
					}
					// gologger.Infof("返回数据: %+v", response)
					c.JSON(200, response)
				})
			}
		}

		// 2. 模型安全中心
		modelSecurity := v1.Group("/model-security")
		{
			// 任务管理
			modelSecurity.Group("/tasks")

			// WebSocket 连接 (原有 /ws 接口迁移)
			modelSecurity.GET("/ws", func(c *gin.Context) {
				wsServer.HandleAIInfraWS(c.Writer, c.Request)
			})
		}

		// 3. AI应用安全中心
		appSecurity := v1.Group("/app-security")
		{
			// 任务管理
			appSecurity.Group("/tasks")

			// MCP 相关 (原有接口迁移)
			mcp := appSecurity.Group("/mcp")
			{
				mcp.GET("/plugins", func(c *gin.Context) {
					mcpPlugins(c.Writer, c.Request)
				})
				mcp.GET("/ws", func(c *gin.Context) {
					wsServer.HandleMcpWS(c.Writer, c.Request)
				})
			}
		}

		// 4. Agent 管理
		// agents := v1.Group("/agents")
	}

	// 保持原有路由的兼容性（重定向到新路由）
	r.GET("/show", func(c *gin.Context) {
		c.Redirect(301, "/api/v1/knowledge/vulnerabilities")
	})
	r.GET("/ws", func(c *gin.Context) {
		c.Redirect(301, "/api/v1/model-security/ws")
	})
	r.GET("/mcp/plugins", func(c *gin.Context) {
		c.Redirect(301, "/api/v1/app-security/mcp/plugins")
	})
	r.GET("/mcp_ws", func(c *gin.Context) {
		c.Redirect(301, "/api/v1/app-security/mcp/ws")
	})

	// 静态文件处理
	r.NoRoute(func(c *gin.Context) {
		assetPath := "static" + c.Request.URL.Path
		if c.Request.URL.Path == "/" {
			assetPath = "static/index.html"
		}

		assetData, err := staticFS.ReadFile(assetPath)
		if err != nil {
			assetData, err = staticFS.ReadFile("static/index.html")
			if err != nil {
				c.String(500, "Internal Server Error")
				return
			}
		}

		mimeType := mime.TypeByExtension(filepath.Ext(assetPath))
		if mimeType == "" {
			mimeType = "text/plain"
		}
		c.Header("Content-Type", mimeType)
		c.Data(200, mimeType, assetData)
	})

	// 启动服务器
	gologger.Infof("Starting WebServer on http://%s\n", options.WebServerAddr)
	if err := r.Run(options.WebServerAddr); err != nil {
		gologger.Fatalf("Could not start WebSocket server: %s\n", err)
	}
}
