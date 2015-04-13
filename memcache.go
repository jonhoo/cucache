package cuckoo

import (
	"encoding/binary"
	"errors"
	"time"
)

type Memval struct {
	Bytes   []byte
	Flags   uint16
	Casid   uint64
	Expires time.Time
}

type MemopResType int
type MemopRes struct {
	T MemopResType
	V interface{}
}

const (
	STORED       MemopResType = iota
	NOT_STORED                = iota
	EXISTS                    = iota
	NOT_FOUND                 = iota
	DELETED                   = iota
	CLIENT_ERROR              = iota
	SERVER_ERROR              = -1
)

type Memop func(Memval, bool) (Memval, MemopRes)

func fset(bytes []byte, flags uint16, expires time.Time) Memop {
	return func(old Memval, _ bool) (m Memval, r MemopRes) {
		m = Memval{bytes, flags, old.Casid + 1, expires}
		r.T = STORED
		return
	}
}

func fadd(bytes []byte, flags uint16, expires time.Time) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_STORED
		if !exists {
			m = Memval{bytes, flags, old.Casid + 1, expires}
			r.T = STORED
		}
		return
	}
}

func freplace(bytes []byte, flags uint16, expires time.Time) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_STORED
		if exists {
			m = Memval{bytes, flags, old.Casid + 1, expires}
			r.T = STORED
		}
		return
	}
}

func fappend(bytes []byte) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			nb := make([]byte, len(old.Bytes)+len(bytes))
			nb = append(nb, old.Bytes...)
			nb = append(nb, bytes...)
			m = Memval{nb, old.Flags, old.Casid, old.Expires}
			r.T = STORED
		}
		return
	}
}

func fprepend(bytes []byte) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			nb := make([]byte, len(old.Bytes)+len(bytes))
			nb = append(nb, bytes...)
			nb = append(nb, old.Bytes...)
			m = Memval{nb, old.Flags, old.Casid, old.Expires}
			r.T = STORED
		}
		return
	}
}

func fcas(bytes []byte, flags uint16, expires time.Time, casid uint64) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			r.T = EXISTS
			if old.Casid == casid {
				m = Memval{bytes, flags, casid + 1, expires}
				r.T = STORED
			}
		}
		return
	}
}

func fpm(by uint64, plus bool) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			if len(old.Bytes) > 8 {
				r.T = CLIENT_ERROR
				r.V = errors.New("")
				return
			}

			v, _ := binary.Uvarint(old.Bytes)
			if plus {
				v += by
			} else {
				v -= by
			}
			nb := make([]byte, 8)
			binary.PutUvarint(nb, v)
			m = Memval{nb, old.Flags, old.Casid + 1, old.Expires}
			r.T = STORED
			r.V = v
		}
		return
	}
}

func fincr(by uint64) Memop {
	return fpm(by, true)
}

func fdecr(by uint64) Memop {
	return fpm(by, false)
}
