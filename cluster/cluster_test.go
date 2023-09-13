package cluster

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/godyy/gnet"
	"github.com/godyy/gserver/cluster/data"
	"github.com/godyy/gserver/cluster/session"
	"github.com/godyy/gutils/log"
	"gopkg.in/yaml.v2"
)

func TestConfig(t *testing.T) {
	config := Config{
		NodeInfo: NodeInfo{
			Uuid:     "node",
			Name:     "node_test",
			Addr:     ":8888",
			Category: "test",
		},
		Service: &session.ServiceConfig{
			Token:                 "token",
			RetryDelayOfListening: 5000,
			HandshakeTimeout:      10000,
			Session: session.Config{
				HeartbeatTimeout: 30000,
				InactiveTimeout:  600000,
				ReadTimeout:      60000,
				WriteTimeout:     60000,
				ReadBufferSize:   8192,
				WriteBufferSize:  8192,
				SendQueueSize:    100,
				MaxPacketSize:    64 * 1024,
			},
		},
		DataDriver: &data.DriverConfig{
			DriverType: "redis",
			Redis: &data.RedisDriverConfig{
				Addr:          []string{"127.0.0.1:6379"},
				Password:      "123456",
				DB:            0,
				PoolSize:      0,
				IsCluster:     false,
				KeyOfNodeInfo: "cluster_node_info",
			},
		},
	}

	configBytes, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}

	os.Mkdir("./bin", os.ModePerm)
	if err := os.WriteFile("./bin/config-test.yaml", configBytes, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	file, err := os.Create("./bin/config-test.toml")
	if err != nil {
		t.Fatal(err)
	}
	if err := toml.NewEncoder(file).Encode(config); err != nil {
		t.Fatal(err)
	}
	file.Close()
}

type testHandler struct {
	logger       log.Logger
	receiveCount *atomic.Int64
	wg           *sync.WaitGroup
}

func (h testHandler) OnSessionPacket(session *session.Session, packet *gnet.Packet) error {
	_, err := packet.ReadVarint()
	if err != nil {
		h.logger.Errorln("testHandler.OnSessionPacket: read packetId", err)
		return err
	}
	//log.Println("testHandler.OnSessionPacket receive packet", packetId)
	h.receiveCount.Add(1)
	h.wg.Done()
	return nil
}

func (h testHandler) OnSessionClosed(session *session.Session) {
	//log.Println("testHandler.OnSessionClosed")
}

func TestCluster(t *testing.T) {
	logger, err := log.CreateLogger(&log.Config{
		Level:           log.DebugLevel,
		EnableCaller:    true,
		CallerSkip:      0,
		Development:     true,
		EnableStdOutput: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	logger.Named("TestCluster")

	connects := new(atomic.Int64)
	packetId := new(atomic.Int64)
	sends := new(atomic.Int64)
	receives := new(atomic.Int64)
	wg := &sync.WaitGroup{}

	serviceConfig := &session.ServiceConfig{
		Token:                 "123",
		RetryDelayOfListening: 5000,
		HandshakeTimeout:      5000,
		Session: session.Config{
			HeartbeatTimeout: 2000,
			InactiveTimeout:  60000,
			ReadTimeout:      30000,
			WriteTimeout:     30000,
			//ReadTimeout:      0,
			//WriteTimeout:     0,
			ReadBufferSize:  64 * 1024,
			WriteBufferSize: 64 * 1024,
			SendQueueSize:   100,
			MaxPacketSize:   16 * 1024,
		},
	}
	dataDriverConfig := &data.DriverConfig{
		DriverType: data.DriverRedis,
		Redis: &data.RedisDriverConfig{
			Addr:          []string{"127.0.0.1:6379"},
			Password:      "",
			DB:            0,
			PoolSize:      0,
			IsCluster:     false,
			KeyOfNodeInfo: "cluster_node_info",
		},
	}

	node1Config := &Config{
		NodeInfo: NodeInfo{
			Uuid:     "node1",
			Name:     "node1",
			Addr:     ":1111",
			Category: "node",
		},
		Service:    serviceConfig,
		DataDriver: dataDriverConfig,
	}
	node1, err := CreateCluster(node1Config, &testHandler{receiveCount: receives, wg: wg}, logger)
	if err != nil {
		logger.Fatal("node1", err)
	}

	node2Config := &Config{
		NodeInfo: NodeInfo{
			Uuid:     "node2",
			Name:     "node2",
			Addr:     ":1112",
			Category: "node",
		},
		Service:    serviceConfig,
		DataDriver: dataDriverConfig,
	}
	node2, err := CreateCluster(node2Config, &testHandler{receiveCount: receives, wg: wg}, logger)
	if err != nil {
		logger.Fatal("node2", err)
	}

	time.Sleep(1 * time.Second)

	const n = 2000
	const k = 1000

	go func() {
		for i := 0; i < n; i++ {
			go func() {
				for j := 0; j < k; j++ {
					connects.Add(1)
					session, err := node1.ConnectNode("node2")
					if err != nil {
						logger.Errorf("node1 connect node2: %s", err)
					} else {
						packetId := packetId.Add(1)
						p := gnet.GetPacket()
						p.WriteVarint(packetId)
						if err := session.SendPacket(p); err != nil {
							logger.Errorf("%s send to %s No.%d: %s", node1Config.NodeInfo.Uuid, node2Config.NodeInfo.Uuid, i, err)
						} else {
							sends.Add(1)
						}
					}
				}
			}()
		}
	}()
	wg.Add(n * k)

	go func() {
		for i := 0; i < n; i++ {
			go func() {
				for j := 0; j < k; j++ {
					connects.Add(1)
					session, err := node2.ConnectNode("node1")
					if err != nil {
						logger.Errorf("node2 connect node1: %s", err)
					} else {
						packetId := packetId.Add(1)
						p := gnet.GetPacket()
						p.WriteVarint(packetId)
						if err := session.SendPacket(p); err != nil {
							logger.Errorf("%s send to %s No.%d: %s", node2Config.NodeInfo.Uuid, node1Config.NodeInfo.Uuid, i, err)
						} else {
							sends.Add(1)
						}
					}
				}
			}()
		}
	}()
	wg.Add(n * k)

	chWg := make(chan struct{})
	go func() {
		wg.Wait()
		close(chWg)
	}()
	chNotify := make(chan os.Signal, 1)
	signal.Notify(chNotify, syscall.SIGINT)
	select {
	case <-chNotify:
	case <-chWg:
	}

	node1.Stop()
	node2.Stop()
	logger.Warnln("connects", connects.Load())
	logger.Warnln("packetId", packetId.Load())
	logger.Warnln("sends", sends.Load())
	logger.Warnln("receives", receives.Load())
}

func TestConcurrentConnect(t *testing.T) {
	logger, err := log.CreateLogger(&log.Config{
		Level:           log.WarnLevel,
		EnableCaller:    true,
		CallerSkip:      0,
		Development:     true,
		EnableStdOutput: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	logger.Named("TestConcurrentConnect")

	connects := new(atomic.Int64)
	packetId := new(atomic.Int64)
	sends := new(atomic.Int64)
	receives := new(atomic.Int64)
	wg := &sync.WaitGroup{}

	handler := &testHandler{receiveCount: receives, wg: wg}

	serviceConfig := &session.ServiceConfig{
		Token:                 "123",
		RetryDelayOfListening: 5000,
		HandshakeTimeout:      5000,
		Session: session.Config{
			HeartbeatTimeout: 2000,
			InactiveTimeout:  60000,
			ReadTimeout:      30000,
			WriteTimeout:     30000,
			//ReadTimeout:      0,
			//WriteTimeout:     0,
			ReadBufferSize:  64 * 1024,
			WriteBufferSize: 64 * 1024,
			SendQueueSize:   100,
			MaxPacketSize:   16 * 1024,
		},
	}
	dataDriverConfig := &data.DriverConfig{
		DriverType: data.DriverRedis,
		Redis: &data.RedisDriverConfig{
			Addr:          []string{"127.0.0.1:6379"},
			Password:      "",
			DB:            0,
			PoolSize:      0,
			IsCluster:     false,
			KeyOfNodeInfo: "cluster_node_info",
		},
	}

	nodeCount := 10
	nodes := make([]*Cluster, nodeCount)
	for i := range nodes {
		node, err := CreateCluster(&Config{
			NodeInfo: NodeInfo{
				Uuid:     fmt.Sprintf("Node%d", i),
				Name:     fmt.Sprintf("Node%d", i),
				Addr:     fmt.Sprintf(":%d", 40000+i),
				Category: "node",
			},
			Service:    serviceConfig,
			DataDriver: dataDriverConfig,
		}, handler, logger)
		if err != nil {
			logger.Fatalf("create node %d", i)
		}
		nodes[i] = node
	}

	time.Sleep(10 * time.Second)

	n := 10
	m := 100
	for i := range nodes {
		wg.Add(n * m * (nodeCount - 1))
		go func(node *Cluster, i int) {
			for k := range nodes {
				if k == i {
					continue
				}
				go func(n1, n2 *Cluster) {
					for i := 0; i < n; i++ {
						connects.Add(1)
						sess, err := n1.ConnectNode(n2.config.NodeInfo.Uuid)
						if err != nil {
							logger.Errorf("%s connect %s: %s", n1.config.NodeInfo.Uuid, n2.config.NodeInfo.Uuid, err)
							return
						}

						go func(session *session.Session) {
							for i := 0; i < m; i++ {
								packetId := packetId.Add(1)
								p := gnet.GetPacket()
								p.WriteVarint(packetId)
								if err := session.SendPacket(p); err != nil {
									logger.Errorf("%s send to %s No.%d: %s", n1.config.NodeInfo.Uuid, n2.config.NodeInfo.Uuid, i, err)
								} else {
									sends.Add(1)
								}
							}
						}(sess)
					}
				}(node, nodes[k])
			}

		}(nodes[i], i)
	}

	chWg := make(chan struct{})
	go func() {
		wg.Wait()
		close(chWg)
	}()
	chNotify := make(chan os.Signal, 1)
	signal.Notify(chNotify, syscall.SIGINT)
	select {
	case <-chNotify:
	case <-chWg:
	}

	for i := range nodes {
		nodes[i].Stop()
	}

	logger.Warnln("connects", connects.Load())
	logger.Warnln("packetId", packetId.Load())
	logger.Warnln("sends", sends.Load())
	logger.Warnln("receives", receives.Load())
}
