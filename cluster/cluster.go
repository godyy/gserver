package cluster

import (
	"github.com/godyy/gserver/cluster/data"
	"github.com/godyy/gserver/cluster/session"
	"github.com/godyy/gutils/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type NodeInfo struct {
	// 结点ID
	Uuid string `yaml:"Uuid"`

	// 结点名称
	Name string `yaml:"Name"`

	// 类型
	Category string `yaml:"Category"`

	// 地址
	Addr string `yaml:"Addr"`
}

type Config struct {
	// 结点信息
	NodeInfo NodeInfo `yaml:"NodeInfo"`

	// 网络服务相关配置
	Service *session.ServiceConfig `yaml:"Service"`

	// 数据驱动相关配置
	DataDriver *data.DriverConfig `yaml:"DataDriver"`
}

type Cluster struct {
	config  *Config          // 配置数据
	service *session.Service // 提供与集群中其他结点的网络交互
	dd      data.Driver      // 集群数据驱动
}

func CreateCluster(config *Config, h session.Handler, logger log.Logger) (*Cluster, error) {
	c := &Cluster{
		config: config,
	}

	logger = logger.
		Named("cluster").
		WithFields(zap.Dict(
			"node",
			zap.String("Uuid", config.NodeInfo.Uuid),
			zap.String("Name", config.NodeInfo.Name),
			zap.String("Category", config.NodeInfo.Category),
			zap.String("Addr", config.NodeInfo.Addr),
		))

	c.service = session.NewService(
		session.ServiceInfo{
			NodeId: config.NodeInfo.Uuid,
			Addr:   config.NodeInfo.Addr,
		},
		config.Service,
		h,
		logger,
	)
	c.service.Start()

	dd, err := data.CreateDriver(config.DataDriver)
	if err != nil {
		return nil, err
	}
	c.dd = dd

	if err := c.updateNodeInfo(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Cluster) Stop() {
	c.service.Close()
}

func (c *Cluster) GetNodeInfo(nodeId string, ni *data.NodeInfo) error {
	return c.dd.LoadNode(nodeId, ni)
}

func (c *Cluster) ConnectNode(nodeId string) (*session.Session, error) {
	if nodeId == c.config.NodeInfo.Uuid {
		return nil, session.ErrConnectSelf
	}

	if sess := c.service.GetSession(nodeId); sess != nil {
		return sess, nil
	}

	var ni data.NodeInfo
	if err := c.GetNodeInfo(nodeId, &ni); err != nil {
		return nil, errors.WithMessage(err, "load NodeInfo")
	}

	return c.service.Connect(nodeId, ni.Addr)
}

func (c *Cluster) updateNodeInfo() error {
	ni := data.NodeInfo{
		Uuid:     c.config.NodeInfo.Uuid,
		Name:     c.config.NodeInfo.Name,
		Addr:     c.config.NodeInfo.Addr,
		Category: c.config.NodeInfo.Category,
	}
	return c.dd.SaveNode(ni.Uuid, &ni)
}