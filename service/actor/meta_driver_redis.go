package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-redis/redis/v8"
	pkg_errors "github.com/pkg/errors"
)

const (
	mdRedisCodeOK           = 0 // 成功
	mdRedisCodeExists       = 1 // 已存在
	mdRedisCodeNotExists    = 2 // 不存在
	mdRedisCodeVersionWrong = 3 // 版本不匹配
)

var mdRedisCode2Err = [...]error{
	mdRedisCodeOK:           nil,
	mdRedisCodeExists:       ErrMDExists,
	mdRedisCodeVersionWrong: ErrMDVersionWrong,
}

var errMDRedisUnknownCode = errors.New("MetaDriver: Redis: unknown code")

// RedisMetaDriver Meta数据驱动 - Redis实现
type RedisMetaDriver struct {
	redisCli      redis.UniversalClient
	metaAutoIdKey string
}

func NewRedisMetaDriver(redisCli redis.UniversalClient, metaAutoIdKey string) *RedisMetaDriver {
	return &RedisMetaDriver{redisCli: redisCli, metaAutoIdKey: metaAutoIdKey}
}

func (md *RedisMetaDriver) genMetaAutoId() (int64, error) {
	return md.redisCli.IncrBy(context.TODO(), md.metaAutoIdKey, 1).Result()
}

var redisScriptAddMeta = redis.NewScript(`
-- KEYS:
--	1: meta key.
-- ARGV:
-- 	1. data value.
--	2. version value.

local ret = redis.call('HMGET', KEYS[1], 'data', 'version')
if 2 ~= #ret or (not ret[1]) or (not ret[2]) then
	redis.call('HSET', KEYS[1], 'data', ARGV[1], 'version', ARGV[2])
	return {0}
else
	return {1, ret[1], ret[2]}
end
`)

func (md *RedisMetaDriver) AddMeta(meta *Meta) (added bool, err error) {
	meta.Id, err = md.genMetaAutoId()
	if err != nil {
		return false, pkg_errors.WithMessage(err, "gen meta auto id")
	}

	var metaBytes []byte
	metaBytes, err = md.encodeMeta(meta)
	if err != nil {
		return
	}

	var ret []string
	ret, err = redisScriptAddMeta.Run(context.TODO(), md.redisCli,
		[]string{md.metaKey(meta.Uuid)},
		string(metaBytes), meta.Version).StringSlice()
	if err != nil {
		return false, err
	}

	var code int
	if code, err = md.decodeCode(ret[0]); err != nil {
		return
	}

	switch code {
	case mdRedisCodeOK:
		if err = md.setMetaUuidById(meta.Id, meta.Uuid); err != nil {
			return
		}
		return true, nil
	case mdRedisCodeExists:
		if err = md.decodeMeta(meta, ret[1]); err != nil {
			return
		}

		var version uint32
		if version, err = md.decodeVersion(ret[2]); err != nil {
			return
		}
		meta.Version = version
		if err = md.setMetaUuidById(meta.Id, meta.Uuid); err != nil {
			return
		}
		return false, nil
	default:
		return false, ErrMDCodeWrong
	}
}

var redisScriptUpdateMeta = redis.NewScript(`
-- KEYS:
--	1: meta key.
-- ARGV:
-- 	1. data value.
--	2. version value.

local ret = redis.call('HMGET', KEYS[1], 'data', 'version')
if 2 ~= #ret or (not ret[1]) or (not ret[2]) then
	return {2}
else if ret[2] ~= ARGV[2] then
	return {3, ret[1], ret[2]}
else
	local version = tonumber(ARGV[2]) + 1
	redis.call('HSET', KEYS[1], 'data', ARGV[1], 'version', version)
	return {0, version}
end
`)

func (md *RedisMetaDriver) UpdateMeta(meta *Meta) (updated bool, err error) {
	var metaBytes []byte
	metaBytes, err = md.encodeMeta(meta)
	if err != nil {
		return
	}

	ret, err := redisScriptUpdateMeta.Run(context.TODO(), md.redisCli,
		[]string{md.metaKey(meta.Uuid)},
		string(metaBytes), meta.Version).StringSlice()
	if err != nil {
		return false, err
	}

	var code int
	code, err = md.decodeCode(ret[0])
	if err != nil {
		return
	}

	switch code {
	case mdRedisCodeOK:
		var version uint32
		version, err = md.decodeVersion(ret[1])
		if err != nil {
			return
		}
		meta.Version = version
		return true, nil
	case mdRedisCodeNotExists:
		return false, ErrMDNotExists
	case mdRedisCodeVersionWrong:
		if err = md.decodeMeta(meta, ret[1]); err != nil {
			return
		}
		var version uint32
		version, err = md.decodeVersion(ret[1])
		if err != nil {
			return
		}
		meta.Version = version
		return false, nil
	default:
		return false, ErrMDCodeWrong
	}
}

var redisScriptGetMeta = redis.NewScript(`
-- KEYS:
--	1: meta key.

local ret = redis.call('HMGET', KEYS[1], 'data', 'version')
if 2 ~= #ret or (not ret[1]) or (not ret[2]) then
	return {2}
else
	return {0, ret[1], ret[2]}
end
`)

func (md *RedisMetaDriver) GetMeta(uuid string, meta *Meta) (err error) {
	var ret []string
	ret, err = redisScriptGetMeta.Run(context.TODO(), md.redisCli, []string{md.metaKey(uuid)}).StringSlice()
	if err != nil {
		return err
	}

	var code int
	code, err = md.decodeCode(ret[0])
	if err != nil {
		return err
	}

	switch code {
	case mdRedisCodeOK:
		if err = md.decodeMeta(meta, ret[1]); err != nil {
			return
		}
		var version uint32
		version, err = md.decodeVersion(ret[1])
		if err != nil {
			return
		}
		meta.Version = version
		return
	case mdRedisCodeNotExists:
		return ErrMDNotExists
	default:
		return ErrMDCodeWrong
	}
}

func (md *RedisMetaDriver) GetMetaById(id int64, meta *Meta) (err error) {
	var uuid string
	uuid, err = md.getMetaUuidById(id)
	if err != nil {
		return err
	}
	return md.GetMeta(uuid, meta)
}

func (md *RedisMetaDriver) getMetaUuidById(id int64) (string, error) {
	uuid, err := md.redisCli.Get(context.TODO(), md.metaUuidKey(id)).Result()
	if err != nil {
		err = pkg_errors.WithMessage(err, "get meta uuid")
	}
	return uuid, err
}

func (md *RedisMetaDriver) setMetaUuidById(id int64, uuid string) error {
	_, err := md.redisCli.Set(context.TODO(), md.metaUuidKey(id), uuid, 0).Result()
	if err != nil {
		err = pkg_errors.WithMessage(err, "set meta uuid")
	}
	return err
}

func (md *RedisMetaDriver) metaKey(uuid string) string {
	return "actor:{" + uuid + "}:meta"
}

func (md *RedisMetaDriver) metaUuidKey(id int64) string {
	return fmt.Sprintf("actor_uuid:%d", id)
}

func (md *RedisMetaDriver) code2Err(code int) error {
	if code < 0 || code >= len(mdRedisCode2Err) {
		return errMDRedisUnknownCode
	}
	return mdRedisCode2Err[code]
}

func (md *RedisMetaDriver) decodeCode(codeStr string) (int, error) {
	if code, err := strconv.Atoi(codeStr); err != nil {
		return 0, pkg_errors.WithMessagef(err, "parse code %s", codeStr)
	} else {
		return code, nil
	}
}

func (md *RedisMetaDriver) decodeVersion(versionStr string) (uint32, error) {
	version, err := strconv.ParseInt(versionStr, 10, 64)
	if err != nil {
		return 0, pkg_errors.WithMessagef(err, "parse version %s", versionStr)
	}
	return uint32(version), nil
}

func (md *RedisMetaDriver) encodeMeta(meta *Meta) ([]byte, error) {
	return json.Marshal(meta)
}

func (md *RedisMetaDriver) decodeMeta(meta *Meta, s string) error {
	if err := json.Unmarshal(([]byte)(s), meta); err != nil {
		return err
	}
	return nil
}
