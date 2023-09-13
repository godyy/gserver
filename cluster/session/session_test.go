package session

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/godyy/gnet"
	"github.com/godyy/gutils/log"
)

type testHandler struct {
	logger       log.Logger
	receiveCount *atomic.Int64
	wg           *sync.WaitGroup
}

func (h testHandler) OnSessionPacket(session *Session, packet *gnet.Packet) error {
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

func (h testHandler) OnSessionClosed(session *Session) {
	//log.Println("testHandler.OnSessionClosed")
}

func TestSession(t *testing.T) {
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
	logger = logger.Named("TestSession")

	connects := new(atomic.Int64)
	packetId := new(atomic.Int64)
	sends := new(atomic.Int64)
	receives := new(atomic.Int64)
	wg := &sync.WaitGroup{}

	sessionConfig := Config{
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
	}

	service1 := NewService(ServiceInfo{
		NodeId: "node1",
		Addr:   ":1111",
	}, &ServiceConfig{
		Token:                 "123",
		RetryDelayOfListening: 5000,
		HandshakeTimeout:      5000,
		Session:               sessionConfig,
	}, &testHandler{receiveCount: receives, wg: wg}, logger)
	if err := service1.Start(); err != nil {
		t.Fatal(err)
	}

	service2 := NewService(ServiceInfo{
		NodeId: "node2",
		Addr:   ":1112",
	}, &ServiceConfig{
		Token:                 "123",
		RetryDelayOfListening: 5000,
		HandshakeTimeout:      5000,
		Session:               sessionConfig,
	}, &testHandler{receiveCount: receives, wg: wg}, logger)
	if err := service2.Start(); err != nil {
		t.Fatal(err)
	}

	logger.Warnf("server1: %+v", service1.config)
	logger.Warnf("server2: %+v", service2.config)

	time.Sleep(1 * time.Second)

	const n = 2000
	const k = 1000

	go func() {
		for i := 0; i < n; i++ {
			go func() {
				for j := 0; j < k; j++ {
					connects.Add(1)
					session, err := service1.Connect("node2", ":1112")
					if err != nil {
						logger.Errorf("node1 connect node2: %s", err)
					} else {
						packetId := packetId.Add(1)
						p := gnet.GetPacket()
						p.WriteVarint(packetId)
						if err := session.SendPacket(p); err != nil {
							logger.Errorf("%s send to %s No.%d: %s", service1.NodeId, service2.NodeId, i, err)
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
					session, err := service2.Connect("node1", ":1111")
					if err != nil {
						logger.Errorf("node2 connect node1: %s", err)
					} else {
						packetId := packetId.Add(1)
						p := gnet.GetPacket()
						p.WriteVarint(packetId)
						if err := session.SendPacket(p); err != nil {
							logger.Errorf("%s send to %s No.%d: %s", service2.NodeId, service1.NodeId, i, err)
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

	service1.Close()
	service2.Close()
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
	logger = logger.Named("TestConcurrentConnect")

	connects := new(atomic.Int64)
	packetId := new(atomic.Int64)
	sends := new(atomic.Int64)
	receives := new(atomic.Int64)
	wg := &sync.WaitGroup{}

	handler := &testHandler{receiveCount: receives, wg: wg}

	sessionConfig := Config{
		HeartbeatTimeout: 2000,
		InactiveTimeout:  60000,
		ReadTimeout:      30000,
		WriteTimeout:     30000,
		ReadBufferSize:   64 * 1024,
		WriteBufferSize:  64 * 1024,
		SendQueueSize:    100,
		MaxPacketSize:    16 * 1024,
	}

	serviceCount := 50
	services := make([]*Service, serviceCount)
	for i := range services {
		services[i] = NewService(ServiceInfo{
			NodeId: fmt.Sprintf("Node%d", i),
			Addr:   fmt.Sprintf(":%d", 40000+i),
		}, &ServiceConfig{
			RetryDelayOfListening: 5000,
			HandshakeTimeout:      500000000,
			Session:               sessionConfig,
		}, handler, logger)
		services[i].Start()
	}

	time.Sleep(2 * time.Second)

	n := 10
	m := 100
	for i := range services {
		wg.Add(n * m * (serviceCount - 1))
		go func(service *Service, i int) {
			for k := range services {
				if k == i {
					continue
				}
				go func(s1, s2 *Service) {
					for i := 0; i < n; i++ {
						connects.Add(1)
						session, err := s1.Connect(s2.NodeId, s2.Addr)
						if err != nil {
							logger.Errorf("%s connect %s: %s", s1.NodeId, s2.NodeId, err)
							return
						}

						go func(session *Session) {
							for i := 0; i < m; i++ {
								packetId := packetId.Add(1)
								p := gnet.GetPacket()
								p.WriteVarint(packetId)
								if err := session.SendPacket(p); err != nil {
									logger.Errorf("%s send to %s No.%d: %s", s1.NodeId, s2.NodeId, i, err)
								} else {
									sends.Add(1)
								}
							}
						}(session)
					}
				}(service, services[k])
			}

		}(services[i], i)
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

	for i := range services {
		services[i].Close()
	}

	logger.Warnln("connects", connects.Load())
	logger.Warnln("packetId", packetId.Load())
	logger.Warnln("sends", sends.Load())
	logger.Warnln("receives", receives.Load())
}