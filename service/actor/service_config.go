package actor

import "time"

const (
	defaultRPCTimeout        = 5000
	defaultActorPersistDelay = 5
	defaultActorExpireDelay  = 1800
)

const (
	unitActorPersistDelay = time.Second
	unitActorExpireDelay  = time.Second
)

type ServiceConfig struct {
	DefaultRPCTimeout int64
	ActorPersistDelay int64
	ActorExpireDelay  int64
}

func (c *ServiceConfig) init() {
	if c.DefaultRPCTimeout == 0 {
		c.DefaultRPCTimeout = defaultRPCTimeout
	}

	if c.ActorPersistDelay == 0 {
		c.ActorPersistDelay = defaultActorPersistDelay
	}
	c.ActorPersistDelay = (time.Duration(c.ActorExpireDelay) * unitActorPersistDelay).Nanoseconds()

	if c.ActorExpireDelay == 0 {
		c.ActorExpireDelay = defaultActorExpireDelay
	}
	c.ActorExpireDelay = (time.Duration(c.ActorExpireDelay) * unitActorExpireDelay).Nanoseconds()
}

func (c *ServiceConfig) GetDefaultRPCTimeout() int64 {
	return c.DefaultRPCTimeout
}

func (c *ServiceConfig) GetActorPersistDelay() int64 {
	return c.ActorPersistDelay
}

func (c *ServiceConfig) GetActorExpireDelay() int64 {
	return c.ActorExpireDelay
}
