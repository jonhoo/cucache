package cuckoo

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Memval is a container for all data a Memcache client may wish to store for a
// particular key.
type Memval struct {
	Bytes   []byte
	Flags   uint32
	Casid   uint64
	Expires time.Time
}

// MemopResType is a status code for the result of a map operation.
type MemopResType int

// MemopRes is a container for the result of a map operation.
type MemopRes struct {
	T MemopResType
	M *Memval
	E error
}

const (
	STORED       MemopResType = iota
	NOT_STORED                = iota
	EXISTS                    = iota
	NOT_FOUND                 = iota
	CLIENT_ERROR              = iota
	SERVER_ERROR              = -1
)

func (t MemopResType) String() string {
	switch t {
	case STORED:
		return "STORED"
	case NOT_STORED:
		return "NOT_STORED"
	case EXISTS:
		return "EXISTS"
	case NOT_FOUND:
		return "NOT_FOUND"
	case CLIENT_ERROR:
		return "CLIENT_ERROR"
	case SERVER_ERROR:
		return "SERVER_ERROR"
	default:
		panic(fmt.Sprintf("unknown type %d\n", t))
	}
}

// Memop is a map operation to be performed for some key.
// The operation will be passed the current value (Memop{} if no value exists),
// and a boolean flag indicating if the key already existed in the database.
// The operation is executed within a lock for the given item.
type Memop func(Memval, bool) (Memval, MemopRes)

// fset returns a Memop that overwrites the current value for a key.
func fset(bytes []byte, flags uint32, expires time.Time) Memop {
	return func(old Memval, _ bool) (m Memval, r MemopRes) {
		m = Memval{bytes, flags, old.Casid + 1, expires}
		r.T = STORED
		r.M = &m
		return
	}
}

// fadd returns a Memop that adds the current value for a non-existing key.
func fadd(bytes []byte, flags uint32, expires time.Time) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = EXISTS
		if !exists {
			m = Memval{bytes, flags, old.Casid + 1, expires}
			r.T = STORED
			r.M = &m
		}
		return
	}
}

// freplace returns a Memop that replaces the current value for an existing key.
func freplace(bytes []byte, flags uint32, expires time.Time) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			m = Memval{bytes, flags, old.Casid + 1, expires}
			r.T = STORED
			r.M = &m
		}
		return
	}
}

// fjoin returns a Memop that prepends or appends the given bytes to the value
// of an existing key. if casid is non-zero, a cas check will be performed.
func fjoin(bytes []byte, prepend bool, casid uint64) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			r.T = EXISTS
			if casid == 0 || old.Casid == casid {
				nb := make([]byte, 0, len(old.Bytes)+len(bytes))
				if prepend {
					nb = append(nb, bytes...)
					nb = append(nb, old.Bytes...)
				} else {
					nb = append(nb, old.Bytes...)
					nb = append(nb, bytes...)
				}
				m = Memval{nb, old.Flags, old.Casid + 1, old.Expires}
				r.T = STORED
				r.M = &m
			}
		}
		return
	}
}

// fappend returns a Memop that appends the given bytes to the value of an
// existing key. if casid is non-zero, a cas check will be performed.
func fappend(bytes []byte, casid uint64) Memop {
	return fjoin(bytes, false, casid)
}

// fprepend returns a Memop that prepends the given bytes to the value of an
// existing key. if casid is non-zero, a cas check will be performed.
func fprepend(bytes []byte, casid uint64) Memop {
	return fjoin(bytes, true, casid)
}

// fcas returns a Memop that overwrites the value of an existing key, assuming
// no write has happened since a get returned the data tagged with casid.
func fcas(bytes []byte, flags uint32, expires time.Time, casid uint64) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			r.T = EXISTS
			if old.Casid == casid {
				m = Memval{bytes, flags, casid + 1, expires}
				r.T = STORED
				r.M = &m
			}
		}
		return
	}
}

// CasPMVal is used to hold both CAS and value for incr/decr operations
type CasVal struct {
	Casid  uint64
	NewVal uint64
}

// fpm returns a Memop that increments or decrements the value of an existing
// key. it assumes the key's value is a 64-bit unsigned integer, and will fail
// if the value is larger than 64 bits. overflow will wrap around. underflow is
// set to 0.
func fpm(by uint64, def uint64, expires time.Time, plus bool) Memop {
	return func(old Memval, exists bool) (m Memval, r MemopRes) {
		r.T = NOT_FOUND
		if exists {
			v, err := strconv.ParseUint(string(old.Bytes), 10, 64)
			if err != nil {
				r.T = CLIENT_ERROR
				r.E = errors.New("non-numeric value found for incr/decr key")
				return
			}

			if plus {
				v += by
			} else {
				if by > v {
					v = 0
				} else {
					v -= by
				}
			}
			m = Memval{[]byte(strconv.FormatUint(v, 10)), old.Flags, old.Casid + 1, old.Expires}
			r.T = STORED
			r.M = &m
		} else {
			// If the counter does not exist, one of two things may
			// happen:
			//
			//  1. If the expiration value is all one-bits
			//     (0xffffffff), the operation will fail with
			//     NOT_FOUND.
			//  2. For all other expiration values, the operation
			//     will succeed by seeding the value for this key
			//     with the provided initial value to expire with
			//     the provided expiration time.  The flags will be
			//     set to zero.
			//
			if expires.Unix() != math.MaxInt64 {
				m.Bytes = []byte(strconv.FormatUint(def, 10))
				m.Expires = expires
				m.Casid = 1
				m.Flags = 0

				r.T = STORED
				r.M = &m
			}
		}
		return
	}
}

// fincr returns a Memop that increments the value of an existing key.
// the value is assumed to be a 64-bit unsigned integer. overflow wraps.
func fincr(by uint64, def uint64, expires time.Time) Memop {
	return fpm(by, def, expires, true)
}

// fdecr returns a Memop that decrements the value of an existing key.
// the value is assumed to be a 64-bit unsigned integer. underflow is set to 0.
func fdecr(by uint64, def uint64, expires time.Time) Memop {
	return fpm(by, def, expires, false)
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
