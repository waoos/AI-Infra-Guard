package monitoring

import (
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.code.oa.com/trpc-go/trpc-go/metrics"
	_ "git.woa.com/trpc-go/trpc-metrics-zhiyan/v2"
)

// 指标组名称常量
const (
	HTTPRequestMetricGroup = "http_request" // HTTP请求指标组
	TaskMetricGroup        = "task"         // 任务指标组
	BusinessMetricGroup    = "business"     // 业务指标组
	ConnectionMetricGroup  = "connection"   // 连接监控指标组
)

// HTTPMetrics HTTP请求监控指标
type HTTPMetrics struct {
	Path       string
	Method     string
	StatusCode int
	Duration   time.Duration
	ClientIP   string
}

// ReportHTTPMetrics 上报HTTP请求监控指标
func ReportHTTPMetrics(metric HTTPMetrics) {
	// 创建指标列表
	var metricList []*metrics.Metrics

	// 1. HTTP请求总量 - 每次请求计数为1，通过维度可以按路径、方法等分组
	metricList = append(metricList,
		metrics.NewMetrics("http_request_api", 1, metrics.PolicySUM))

	// 2. 请求响应时间 - 记录响应时间
	metricList = append(metricList,
		metrics.NewMetrics("http_response_time", metric.Duration.Seconds(), metrics.PolicyAVG))

	// 创建多维度指标
	rec := metrics.NewMultiDimensionMetrics(
		[]*metrics.Dimension{
			{Name: "ip", Value: metric.ClientIP},
			{Name: "path", Value: metric.Path},
			{Name: "method", Value: metric.Method},
		},
		metricList)

	// 设置自定义指标组名称
	rec.Name = HTTPRequestMetricGroup

	// 上报指标
	err := metrics.Report(rec)
	if err != nil {
		log.Errorf("Report HTTP metrics error: %+v", err)
	}
}

// getStatusCategory 获取状态码分类
func getStatusCategory(statusCode, min, max int) string {
	if statusCode >= min && statusCode <= max {
		return "true"
	}
	return "false"
}

// ReportTaskMetrics 上报任务监控指标
func ReportTaskMetrics(taskType, status string, duration time.Duration, jobID string) {
	var metricList []*metrics.Metrics

	// 1. 任务完成时间 - 记录任务执行时间
	metricList = append(metricList,
		metrics.NewMetrics("task_completion_time", duration.Seconds(), metrics.PolicyAVG))

	// 2. 任务成功率 - 根据状态计算成功率
	successValue := 0.0
	if status == "success" || status == "completed" {
		successValue = 1.0
	}
	metricList = append(metricList,
		metrics.NewMetrics("task_success_rate", successValue, metrics.PolicyAVG))

	// 创建多维度指标
	rec := metrics.NewMultiDimensionMetrics(
		[]*metrics.Dimension{
			{Name: "task_type", Value: taskType},
			{Name: "status", Value: status},
			{Name: "job_id", Value: jobID},
		},
		metricList)

	// 设置指标组名称
	rec.Name = TaskMetricGroup

	// 上报指标
	err := metrics.Report(rec)
	if err != nil {
		log.Errorf("job_id: [%s]. Report task metrics error: %+v", jobID, err)
	}
}

// ConnectionMetrics 连接监控指标
type ConnectionMetrics struct {
	ConnectionType string  // sse 或 websocket
	ConnectionID   string  // 连接ID
	Quality        float64 // 连接质量 (0-1)
	ClientIP       string  // 客户端IP
}

// ReportConnectionMetrics 上报连接监控指标
func ReportConnectionMetrics(metric ConnectionMetrics) {
	var metricList []*metrics.Metrics

	// 1. SSE连接个数 - 计数连接数量
	if metric.ConnectionType == "sse" {
		metricList = append(metricList,
			metrics.NewMetrics("sse_connection_count", 1, metrics.PolicySUM))
	}

	// 2. SSE连接质量 - 记录连接质量
	if metric.ConnectionType == "sse" {
		metricList = append(metricList,
			metrics.NewMetrics("sse_connection_quality", metric.Quality, metrics.PolicyAVG))
	}

	// 3. WebSocket连接质量 - 记录连接质量
	if metric.ConnectionType == "websocket" {
		metricList = append(metricList,
			metrics.NewMetrics("websocket_connection_quality", metric.Quality, metrics.PolicyAVG))
	}

	// 创建多维度指标
	rec := metrics.NewMultiDimensionMetrics(
		[]*metrics.Dimension{
			{Name: "ip", Value: metric.ClientIP},
			{Name: "connection_type", Value: metric.ConnectionType},
			{Name: "connection_id", Value: metric.ConnectionID},
		},
		metricList)

	// 设置指标组名称
	rec.Name = ConnectionMetricGroup

	// 上报指标
	err := metrics.Report(rec)
	if err != nil {
		log.Errorf("connection_id: [%s]. Report connection metrics error: %+v", metric.ConnectionID, err)
	}
}
