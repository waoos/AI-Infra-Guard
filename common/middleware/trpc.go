package middleware

import (
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/codec"
	"github.com/gin-gonic/gin"
)

// TrpcMiddleware 创建trpc-go集成中间件
func TrpcMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 保存原始context
		savedCtx := c.Request.Context()

		// 创建新的trpc message，用于监控、全链路跟踪等场景
		ctx, msg := codec.WithNewMessage(savedCtx)

		// 设置trpc message信息
		msg.WithCalleeApp(trpc.GlobalConfig().Server.App)
		msg.WithCalleeServer(trpc.GlobalConfig().Server.Server)
		msg.WithCalleeService("")
		msg.WithCalleeServiceName("")
		msg.WithCalleeMethod(":" + c.FullPath()) // 避免天机阁 CleanRPCMethod 在请求方法名前加:
		msg.WithCalleeContainerName(trpc.GlobalConfig().Global.ContainerName)

		// 通过request context传递span、trpc信息
		c.Request = c.Request.WithContext(ctx)

		// 继续处理请求
		c.Next()
	}
}
