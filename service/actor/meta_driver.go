package actor

import (
	"errors"
)

var (
	ErrMDVersionWrong = errors.New("MetaDriver: version wrong")
	ErrMDExists       = errors.New("MetaDriver: exists")
	ErrMDNotExists    = errors.New("MetaDriver: not exists")
	ErrMDCodeWrong    = errors.New("MetaDriver: code wrong")
)

// MetaDriver Meta数据驱动
type MetaDriver interface {
	AddMeta(meta *Meta) (bool, error)
	UpdateMeta(meta *Meta) (bool, error)
	GetMeta(uuid string, meta *Meta) error
	GetMetaById(id int64, meta *Meta) error
}
