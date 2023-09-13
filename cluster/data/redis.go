package data

import (
	"context"
	"encoding/json"

	"github.com/go-redis/redis/v8"
)

const DriverRedis = "redis"

type RedisDriverConfig struct {
	// redis服务地址
	// 非cluster模式读取[0]
	Addr []string `yaml:"Addr"`

	// redis服务密码
	Password string `yaml:"Password"`

	// redis服务器数据库编号
	DB int `yaml:"DB"`

	// redis客户端连接池大小
	PoolSize int `yaml:"PoolSize"`

	// 是否cluster模式
	IsCluster bool `yaml:"IsCluster"`

	// 用于读取/保存结点信息的redis-key
	KeyOfNodeInfo string `yaml:"KeyOfNodeInfo"`
}

type RedisDriver struct {
	config   *RedisDriverConfig
	redisCli redis.UniversalClient
	ctx      context.Context
}

func NewRedisDriver(config *RedisDriverConfig) *RedisDriver {
	dd := &RedisDriver{
		config: config,
		ctx:    context.Background(),
	}

	if config.IsCluster {
		dd.redisCli = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    config.Addr,
			Password: config.Password,
			PoolSize: config.PoolSize,
		})
	} else {
		dd.redisCli = redis.NewClient(&redis.Options{
			Addr:     config.Addr[0],
			Password: config.Password,
			PoolSize: config.PoolSize,
		})
	}

	return dd
}

func (d *RedisDriver) LoadNode(nodeId string, info *NodeInfo) error {
	niString, err := d.redisCli.HGet(d.ctx, d.config.KeyOfNodeInfo, nodeId).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal(([]byte)(niString), info)
}

func (d *RedisDriver) SaveNode(nodeId string, info *NodeInfo) error {
	niBytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	_, err = d.redisCli.HSet(d.ctx, d.config.KeyOfNodeInfo, nodeId, string(niBytes)).Result()
	return err
}
