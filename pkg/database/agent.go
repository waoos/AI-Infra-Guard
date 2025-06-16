package database

import (
	"database/sql"
	"encoding/json"
	"time"
)

// Agent 表示一个注册的agent
type Agent struct {
	ID           string    `json:"agent_id"`
	Hostname     string    `json:"hostname"`
	IP           string    `json:"ip"`
	Version      string    `json:"version"`
	Capabilities []string  `json:"capabilities"`
	Meta         string    `json:"meta"` // 存储为JSON字符串
	LastSeen     time.Time `json:"last_seen"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// AgentStore 提供agent相关的数据库操作
type AgentStore struct {
	db *sql.DB
}

// NewAgentStore 创建一个新的AgentStore实例
func NewAgentStore(db *sql.DB) *AgentStore {
	return &AgentStore{db: db}
}

// Init 初始化agent表
func (s *AgentStore) Init() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS agents (
			id TEXT PRIMARY KEY,
			hostname TEXT NOT NULL,
			ip TEXT NOT NULL,
			version TEXT,
			capabilities TEXT,
			meta TEXT NOT NULL,
			last_seen TIMESTAMP NOT NULL,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)
	`)
	return err
}

// Register 注册或更新agent信息
func (s *AgentStore) Register(agent *Agent) error {
	capabilities, err := json.Marshal(agent.Capabilities)
	if err != nil {
		return err
	}

	now := time.Now()
	_, err = s.db.Exec(`
		INSERT INTO agents (id, hostname, ip, version, capabilities, meta, last_seen, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			hostname = excluded.hostname,
			ip = excluded.ip,
			version = excluded.version,
			capabilities = excluded.capabilities,
			meta = excluded.meta,
			last_seen = excluded.last_seen,
			updated_at = excluded.updated_at
	`,
		agent.ID,
		agent.Hostname,
		agent.IP,
		agent.Version,
		string(capabilities),
		agent.Meta,
		now,
		now,
		now,
	)
	return err
}

// GetAgent 获取指定agent的信息
func (s *AgentStore) GetAgent(id string) (*Agent, error) {
	var agent Agent
	var capabilities string
	err := s.db.QueryRow(`
		SELECT id, hostname, ip, version, capabilities, meta, last_seen, created_at, updated_at
		FROM agents WHERE id = ?
	`, id).Scan(
		&agent.ID,
		&agent.Hostname,
		&agent.IP,
		&agent.Version,
		&capabilities,
		&agent.Meta,
		&agent.LastSeen,
		&agent.CreatedAt,
		&agent.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(capabilities), &agent.Capabilities)
	if err != nil {
		return nil, err
	}

	return &agent, nil
}

// ListAgents 获取所有agent列表
func (s *AgentStore) ListAgents() ([]*Agent, error) {
	rows, err := s.db.Query(`
		SELECT id, hostname, ip, version, capabilities, meta, last_seen, created_at, updated_at
		FROM agents
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var agents []*Agent
	for rows.Next() {
		var agent Agent
		var capabilities string
		err := rows.Scan(
			&agent.ID,
			&agent.Hostname,
			&agent.IP,
			&agent.Version,
			&capabilities,
			&agent.Meta,
			&agent.LastSeen,
			&agent.CreatedAt,
			&agent.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal([]byte(capabilities), &agent.Capabilities)
		if err != nil {
			return nil, err
		}

		agents = append(agents, &agent)
	}

	return agents, nil
}

// UpdateLastSeen 更新agent的最后在线时间
func (s *AgentStore) UpdateLastSeen(id string) error {
	_, err := s.db.Exec(`
		UPDATE agents SET last_seen = ?, updated_at = ?
		WHERE id = ?
	`, time.Now(), time.Now(), id)
	return err
}

// DeleteAgent 删除指定的agent
func (s *AgentStore) DeleteAgent(id string) error {
	_, err := s.db.Exec("DELETE FROM agents WHERE id = ?", id)
	return err
}
