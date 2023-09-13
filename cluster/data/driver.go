package data

import (
	"errors"
	"strings"
)

type DriverConfig struct {
	// 驱动类型
	// redis | ...
	DriverType string `yaml:"DriverType"`

	// Redis驱动配置
	Redis *RedisDriverConfig `yaml:"Redis,omitempty"`
}

type Driver interface {
	// LoadNode 读取结点信息
	LoadNode(nodeId string, ni *NodeInfo) error

	// SaveNode 保存结点信息
	SaveNode(nodeId string, ni *NodeInfo) error
}

var ErrInvalidDriverType = errors.New("invalid driver type")

// CreateDriver 根据配置创建驱动
func CreateDriver(config *DriverConfig) (Driver, error) {
	switch strings.ToLower(config.DriverType) {
	case DriverRedis:
		return NewRedisDriver(config.Redis), nil
	default:
		return nil, ErrInvalidDriverType
	}
}
