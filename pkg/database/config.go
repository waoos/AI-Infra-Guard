package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/glebarez/sqlite"
)

// Config 用于保存数据库配置
type Config struct {
	DBPath string
}

// NewConfig 创建一个新的数据库配置
func NewConfig(dbPath string) *Config {
	return &Config{DBPath: dbPath}
}

// InitDB 初始化数据库连接并返回 *sql.DB
func InitDB(config *Config) (*sql.DB, error) {
	// 确保数据库目录存在
	dir := filepath.Dir(config.DBPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据库目录失败: %v", err)
	}

	// 连接数据库
	db, err := sql.Open(sqlite.DriverName, config.DBPath)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("数据库连接测试失败: %v", err)
	}

	return db, nil
}
