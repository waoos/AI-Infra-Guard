package websocket

// 任务创建请求结构体
// 对应前端创建任务时的输入
// 例如：{"id":..., "sessionId":..., "task":..., ...}
type TaskCreateRequest struct {
	ID             string                 `json:"id"`             // 消息ID（前端生成的对话ID）
	SessionID      string                 `json:"sessionId"`      // 会话ID（任务ID）
	Username       string                 `json:"username"`       // 用户名（可选，不传默认为公共用户）
	Task           string                 `json:"task"`           // 任务类型
	Timestamp      int64                  `json:"timestamp"`      // 时间戳
	Content        string                 `json:"content"`        // 任务内容描述
	Params         map[string]interface{} `json:"params"`         // 任务参数
	Attachments    []string               `json:"attachments"`    // 附件列表
	CountryIsoCode string                 `json:"countryIsoCode"` // 国家代码（可选）
}

// 通用事件消息体（SSE推送）
type TaskEventMessage struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	SessionID string      `json:"sessionId"`
	Timestamp int64       `json:"timestamp"`
	Event     interface{} `json:"event"`
}

// liveStatus 事件体
// {"type":"liveStatus", ...}
type LiveStatusEvent struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Text      string `json:"text"`
}

// planUpdate 事件体
type PlanUpdateEvent struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Timestamp int64          `json:"timestamp"`
	Tasks     []PlanTaskItem `json:"tasks"`
}

type PlanTaskItem struct {
	Status    string `json:"status"`
	Title     string `json:"title"`
	StartedAt int64  `json:"startedAt"`
}

// newPlanStep 事件体
type NewPlanStepEvent struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	StepID    string `json:"stepId"`
	Title     string `json:"title"`
}

// statusUpdate 事件体
type StatusUpdateEvent struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Timestamp   int64  `json:"timestamp"`
	AgentStatus string `json:"agentStatus"`
	Brief       string `json:"brief"`
	Description string `json:"description"`
	NoRender    bool   `json:"noRender"`
	PlanStepID  string `json:"planStepId"`
}

// toolUsed 事件体（支持多工具并行）
type ToolUsedEvent struct {
	ID          string      `json:"id"`
	Type        string      `json:"type"`
	Timestamp   int64       `json:"timestamp"`
	Description string      `json:"description"`
	PlanStepID  string      `json:"planStepId"`
	StatusID    string      `json:"statusId"` // 关联的状态，一个状态可以对应多个插件
	Tools       []ToolInfo  `json:"tools"`    // 工具列表
	Detail      interface{} `json:"detail"`   // 详细信息
}

// 工具信息
type ToolInfo struct {
	ToolID  string      `json:"toolId"`
	Tool    string      `json:"tool"`
	Status  string      `json:"status"` // done, doing, failed
	Brief   string      `json:"brief"`
	Message interface{} `json:"message"`
	Result  string      `json:"result"` // 最终执行结果
}

// actionLog 事件体
type ActionLogEvent struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	Timestamp  int64  `json:"timestamp"`
	ActionID   string `json:"actionId"`
	Tool       string `json:"tool"`
	PlanStepID string `json:"planStepId"`
	ActionLog  string `json:"actionLog"`
}

// 任务分配消息（Server -> Agent）
type TaskAssignMessage struct {
	Type    string      `json:"type"`
	Content TaskContent `json:"content"`
}

// 任务内容
type TaskContent struct {
	SessionID   string                 `json:"session_id"`
	TaskType    string                 `json:"task_type"`
	Content     string                 `json:"content"`
	Params      map[string]interface{} `json:"params"`
	Attachments []string               `json:"attachments"`
	Timeout     int                    `json:"timeout"`
}

// 任务更新请求结构体
type TaskUpdateRequest struct {
	Title string `json:"title"` // 任务标题
}
