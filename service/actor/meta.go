package actor

import (
	"errors"
	"fmt"
	"time"

	cluster_data "github.com/godyy/gserver/cluster/data"

	pkg_errors "github.com/pkg/errors"
)

var ErrActorNeedDeployed = errors.New("actor need deployed")

// GetMeta 获取Actor的元信息
func (s *Service) GetMeta(uuid string, meta *Meta) error {
	return s.md.GetMeta(uuid, meta)
}

func (s *Service) GetMetaById(id int64, meta *Meta) error {
	return s.md.GetMetaById(id, meta)
}

// AddMeta 通过Meta信息添加Actor
func (s *Service) AddMeta(meta *Meta) (added bool, err error) {
	if err = s.checkMeta(meta); err != nil {
		return false, err
	}

	return s.md.AddMeta(meta)
}

// DeployMeta 通过Meta信息部署Actor
func (s *Service) DeployMeta(meta *Meta) (err error) {
	if meta.IsDeployed() {
		return nil
	}

	// 检查部署参数
	nodeId := ""
	nodeId, err = s.checkMetaDeploy(meta)
	if err != nil {
		return err
	}

	if meta.Deploy.Mode == DeployModeRecommend {
		// todo

		var ni cluster_data.NodeInfo
		if err := s.cluster.GetNodeInfo(nodeId, &ni); err != nil {
			return pkg_errors.WithMessagef(err, "get info of node %v", nodeId)
		}
	}

	meta.NodeId = nodeId
	if _, err = s.md.UpdateMeta(meta); err != nil {
		return err
	}

	return nil
}

// GetNodeInfoOfMeta 获取Actor所在结点信息
func (s *Service) GetNodeInfoOfMeta(meta *Meta, ni *cluster_data.NodeInfo) error {
	nodeId, err := s.GetNodeOfMeta(meta)
	if err != nil {
		return pkg_errors.WithMessage(err, "get nodeId")
	}

	return s.cluster.GetNodeInfo(nodeId, ni)
}

// GetNodeOfMeta 获取Actor所在结点
func (s *Service) GetNodeOfMeta(meta *Meta) (string, error) {
	if !meta.IsDeployed() {
		return "", ErrActorNeedDeployed
	}

	return meta.NodeId, nil
}

func (s *Service) checkMeta(meta *Meta) (err error) {
	// 检查category
	if !s.IsCategoryExists(meta.Category) {
		return ErrActorCategoryNotExists
	}

	// 检查部署参数
	if _, err = s.checkMetaDeploy(meta); err != nil {
		return err
	}

	return nil
}

func (s *Service) checkMetaDeploy(meta *Meta) (nodeId string, err error) {
	// 检查部署参数
	if err = meta.Deploy.check(); err != nil {
		return "", err
	}
	switch meta.Deploy.Mode {
	case DeployModeOnNode:
		var ni cluster_data.NodeInfo
		if err = s.cluster.GetNodeInfo(meta.Deploy.Value, &ni); err != nil {
			return "", pkg_errors.WithMessage(err, "deploy on node: get node info")
		}
		nodeId = meta.Deploy.Value
	case DeployModeFollowOther:
		var otherMeta Meta
		if err = s.md.GetMeta(meta.Deploy.Value, &otherMeta); err != nil {
			return "", pkg_errors.WithMessage(err, "deploy follow other: get other meta")
		}
		if otherMeta.Deploy.Mode != DeployModeOnNode {
			return "", errors.New("deploy follow other: other deploy mode invalid")
		}
		if !otherMeta.IsDeployed() {
			return "", errors.New("deploy follow other: other not deployed")
		}
		nodeId = otherMeta.NodeId
	case DeployModeRecommend:
		// todo
	default:
		return "", fmt.Errorf("deploy mode %d not implemented", meta.Deploy.Mode)
	}

	return
}

// Meta Actor元信息
type Meta struct {
	Id        int64      // 数字唯一ID
	Uuid      string     // actor唯一ID
	Category  string     // actor的分类，用于匹配actor构造器
	CreatedAt int64      // actor的创建时间
	UpdatedAt int64      // actor元数据更新时间
	NodeId    string     // actor所处结点ID
	Deploy    Deployment // actor的部署信息
	ActiveAt  int64      // actor最近一次活跃时间
	Version   uint32     // 版本号
}

func NewMeta(uuid string, category string, deploy Deployment) Meta {
	nowUnix := time.Now().Unix()
	return Meta{
		Id:        0,
		Uuid:      uuid,
		Category:  category,
		CreatedAt: nowUnix,
		UpdatedAt: nowUnix,
		NodeId:    "",
		Deploy:    deploy,
		ActiveAt:  0,
	}
}

func (m *Meta) IsDeployed() bool {
	if m.NodeId == "" {
		return false
	}

	switch m.Deploy.Mode {
	case DeployModeRecommend:
		if time.Now().Unix()-m.ActiveAt >= 1800 {
			// todo 超过30分钟未活跃
			return false
		}
	}

	return true
}

const (
	DeployModeOnNode      = 0 // 指定结点部署
	DeployModeFollowOther = 1 // 跟随其它actor部署
	DeployModeRecommend   = 2 // 使用负载均衡策略部署
)

var (
	ErrDeployModeInvalid = errors.New("deploy mode invalid")
	ErrDeployValueEmpty  = errors.New("deploy mode empty")
)

type Deployment struct {
	Mode  int    // 部署模式
	Value string // 定义跟随Mode改变
}

func (d *Deployment) check() error {
	if d.Mode < DeployModeOnNode || d.Mode > DeployModeRecommend {
		return ErrDeployModeInvalid
	}
	if d.Value == "" {
		return ErrDeployValueEmpty
	}
	return nil
}
