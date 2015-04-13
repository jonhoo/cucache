package cuckoo

import (
	"encoding/binary"
	"errors"
	"time"
)

// Memval is a container for all data a Memcache client may wish to store for a
// particular key.
type Memval struct {
	Bytes   []byte
	Flags   uint16
	Casid   uint64
	Expires time.Time
}

// MemopResType is a status code for the result of a map operation.
type MemopResType int

// MemopRes is a container for the result of a map operation.
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

// Memop is a map operation to be performed for some key.
// The operation will be passed the current value (Memop{} if no value exists),
// and a boolean flag indicating if the key already existed in the database.
// The operation is executed within a lock for the given item.
type Memop func(Memval, bool) (Memval, MemopRes)

// fset returns a Memop that overwrites the current value for a key.
func fset(bytes []byte, flags uint16, expires time.Time) Memop {
	return func(old Memval, _ bool) (m Memval, r MemopRes) {
		m = Memval{bytes, flags, old.Casid + 1, expires}
		r.T = STORED
		return
	}
}

// fadd returns a Memop that adds the current value for a non-existing key.
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

// freplace returns a Memop that replaces the current value for an existing key.
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

// fappend returns a Memop that appends the given bytes to the value of an
// existing key.
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

// fprepend returns a Memop that prepends the given bytes to the value of an
// existing key.
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

// fcas returns a Memop that overwrites the value of an existing key, assuming
// no write has happened since a get returned the data tagged with casid.
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

// fpm returns a Memop that increments or decrements the value of an existing
// key. it assumes the key's value is a 64-bit unsigned integer, and will fail
// if the value is larger than 64 bits. overflow will wrap around. underflow is
// set to 0.
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
				if by > v {
					v = 0
				} else {
					v -= by
				}
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

// fincr returns a Memop that increments the value of an existing key.
// the value is assumed to be a 64-bit unsigned integer. overflow wraps.
func fincr(by uint64) Memop {
	return fpm(by, true)
}

// fdecr returns a Memop that decrements the value of an existing key.
// the value is assumed to be a 64-bit unsigned integer. underflow is set to 0.
func fdecr(by uint64) Memop {
	return fpm(by, false)
}

// ftouch returns a Memop that updates the expiration time of the given key.
func ftouch(expires time.Time) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			m = Memval{old.Bytes, old.Flags, old.Casid, expires}
			r.T = STORED
		}
		return
	}
}
