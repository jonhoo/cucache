// package cuckoo provides a Memcache-like interface to a concurrent, in-memory
// Cuckoo hash map.
package cuckoo

import (
	"fmt"
	"sync/atomic"
	"time"
)

// MAX_HASHES indicates the maximum number of Cuckoo hashes permitted.
// A higher number will increase the capacity of the map, but will slow down
// operations.
const MAX_HASHES = 10

// Cuckoo is an externally visible wrapper to a Cuckoo map implementation
type Cuckoo struct{ *cmap }

// New produces a new Cuckoo map.
// By default, 2 hashes are used, and the map can hold 1e4 elements. The map
// will automatically increase the number of hashes as the map fills to avoid
// spilling items.
func New() Cuckoo {
	m := new(cmap)
	m.bins = make([]cbin, 1e5 /* TODO: all of main memory */)
	m.hashes = 2
	return Cuckoo{m}
}

// Set overwites the given key
func (c Cuckoo) Set(key []byte, bytes []byte, flags uint32, expires time.Time) MemopRes {
	return c.op(key, fset(bytes, flags, expires))
}

// Add adds a non-existing key
func (c Cuckoo) Add(key []byte, bytes []byte, flags uint32, expires time.Time) MemopRes {
	return c.op(key, fadd(bytes, flags, expires))
}

// Replace replaces an existing key
func (c Cuckoo) Replace(key []byte, bytes []byte, flags uint32, expires time.Time) MemopRes {
	return c.op(key, freplace(bytes, flags, expires))
}

// Append adds to an existing key
func (c Cuckoo) Append(key []byte, bytes []byte, casid uint64) MemopRes {
	return c.op(key, fappend(bytes, casid))
}

// Prepend adds to the beginning of an existing key
func (c Cuckoo) Prepend(key []byte, bytes []byte, casid uint64) MemopRes {
	return c.op(key, fprepend(bytes, casid))
}

// CAS overwrites the value for a key if it has not changed
func (c Cuckoo) CAS(key []byte, bytes []byte, flags uint32, expires time.Time, casid uint64) MemopRes {
	return c.op(key, fcas(bytes, flags, expires, casid))
}

// Incr increments the value for an existing key
func (c Cuckoo) Incr(key []byte, by uint64, def uint64, expires time.Time) MemopRes {
	return c.op(key, fincr(by, def, expires))
}

// Decr decrements the value for an existing key
func (c Cuckoo) Decr(key []byte, by uint64, def uint64, expires time.Time) MemopRes {
	return c.op(key, fdecr(by, def, expires))
}

// Touch updates the expiration time for an existing key
func (c Cuckoo) Touch(key []byte, expires time.Time) MemopRes {
	return c.op(key, ftouch(expires))
}

// TouchAll updates the expiration time for all entries
func (c Cuckoo) TouchAll(expires time.Time) {
	c.touchall(expires)
}

// op executes a particular Memop on the given key.
// it will automatically increase the number of hashes when the map starts to
// become overloaded, but may fail if the map becomes too large.
func (c Cuckoo) op(key []byte, upd Memop) MemopRes {
	h := c.hashes
	res := c.insert(keyt(key), upd)

	for res.T == SERVER_ERROR && h < MAX_HASHES {
		sw := atomic.CompareAndSwapUint32(&c.hashes, h, h+1)
		if sw {
			fmt.Println("insert failed on key", key, ", so upped # hashes to", h+1)
		}

		h = c.hashes
		res = c.insert(keyt(key), upd)
		if sw && res.T == SERVER_ERROR {
			return res
		}
	}

	return res
}

// Delete removes the value for the given key
func (c Cuckoo) Delete(key []byte, casid uint64) MemopRes {
	return c.del(keyt(key), casid)
}

// Get returns the current value for the given key
func (c Cuckoo) Get(key []byte) (*Memval, bool) {
	v := c.get(keyt(key))
	if v.T == NOT_FOUND {
		return nil, false
	}
	return v.M, true
}

// Iterate returns a list of Memvals present in the map
func (c Cuckoo) Iterate() <-chan *Memval {
	ch := make(chan *Memval)
	go func() {
		for v := range c.iterate() {
			ch <- &v.val
		}
		close(ch)
	}()
	return ch
}

// Iterate returns a list of keys present in the map
func (c Cuckoo) IterateKeys() <-chan string {
	ch := make(chan string)
	go func() {
		for v := range c.iterate() {
			ch <- string(v.key)
		}
		close(ch)
	}()
	return ch
}
