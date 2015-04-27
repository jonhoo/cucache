// package cuckoo provides a Memcache-like interface to a concurrent, in-memory
// Cuckoo hash map.
package cuckoo

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// MAX_HASHES indicates the maximum number of Cuckoo hashes permitted.
// A higher number will increase the capacity of the map, but will slow down
// operations.
const MAX_HASHES = 10

// Cuckoo is an externally visible wrapper to a Cuckoo map implementation
type Cuckoo struct {
	resize sync.RWMutex
	cmap   unsafe.Pointer
}

// New produces a new Cuckoo map.
// By default, 2 hashes are used. If esize is 0, the map will be initialized to
// hold 1e4 elements. The map will automatically increase the number of hashes
// as the map fills to avoid spilling items.
func New(esize int) Cuckoo {
	if esize == 0 {
		esize = 1e4
	}
	return Cuckoo{sync.RWMutex{}, unsafe.Pointer(create(esize))}
}

func (c Cuckoo) get() *cmap {
	return (*cmap)(atomic.LoadPointer(&c.cmap))
}

// Set overwites the given key
func (c *Cuckoo) Set(key []byte, bytes []byte, flags uint32, expires time.Time) MemopRes {
	return c.op(key, fset(bytes, flags, expires))
}

// Add adds a non-existing key
func (c *Cuckoo) Add(key []byte, bytes []byte, flags uint32, expires time.Time) MemopRes {
	return c.op(key, fadd(bytes, flags, expires))
}

// Replace replaces an existing key
func (c *Cuckoo) Replace(key []byte, bytes []byte, flags uint32, expires time.Time) MemopRes {
	return c.op(key, freplace(bytes, flags, expires))
}

// Append adds to an existing key
func (c *Cuckoo) Append(key []byte, bytes []byte, casid uint64) MemopRes {
	return c.op(key, fappend(bytes, casid))
}

// Prepend adds to the beginning of an existing key
func (c *Cuckoo) Prepend(key []byte, bytes []byte, casid uint64) MemopRes {
	return c.op(key, fprepend(bytes, casid))
}

// CAS overwrites the value for a key if it has not changed
func (c *Cuckoo) CAS(key []byte, bytes []byte, flags uint32, expires time.Time, casid uint64) MemopRes {
	return c.op(key, fcas(bytes, flags, expires, casid))
}

// Incr increments the value for an existing key
func (c *Cuckoo) Incr(key []byte, by uint64, def uint64, expires time.Time) MemopRes {
	return c.op(key, fincr(by, def, expires))
}

// Decr decrements the value for an existing key
func (c *Cuckoo) Decr(key []byte, by uint64, def uint64, expires time.Time) MemopRes {
	return c.op(key, fdecr(by, def, expires))
}

// Touch updates the expiration time for an existing key
func (c *Cuckoo) Touch(key []byte, expires time.Time) MemopRes {
	return c.op(key, ftouch(expires))
}

// TouchAll updates the expiration time for all entries
func (c *Cuckoo) TouchAll(expires time.Time) {
	c.resize.RLock()
	defer c.resize.RUnlock()
	c.get().touchall(expires)
}

// op executes a particular Memop on the given key.
// it will automatically increase the number of hashes when the map starts to
// become overloaded, but may fail if the map becomes too large.
func (c *Cuckoo) op(key []byte, upd Memop) MemopRes {
	c.resize.RLock()

	m := c.get()
	h := m.hashes
	res := m.insert(keyt(key), upd)

	for res.T == SERVER_ERROR && h < MAX_HASHES {
		sw := atomic.CompareAndSwapUint32(&m.hashes, h, h+1)
		if sw {
			fmt.Fprintln(os.Stderr, "insert failed on key", string(key), ", so upped # hashes to", h+1)
		}

		h = m.hashes
		res = m.insert(keyt(key), upd)
	}

	c.resize.RUnlock()

	if res.T == SERVER_ERROR && h == MAX_HASHES {
		c.resize.Lock()
		if c.get() == m {
			fmt.Fprintln(os.Stderr, "insert failed on key", string(key), ", so growing hashtable to", 2*len(m.bins))

			// no one else has already done a resize
			newm := create(2 * len(m.bins))

			i := 0
			var res MemopRes
			// TODO: do in parallel
			for v := range m.iterate() {
				// TODO: keep CAS values
				res = newm.insert(v.key, fadd(v.val.Bytes, v.val.Flags, v.val.Expires))
				if res.T != STORED {
					atomic.AddUint32(&newm.hashes, 1)
					res = newm.insert(v.key, fadd(v.val.Bytes, v.val.Flags, v.val.Expires))
					if res.T != STORED {
						panic(fmt.Sprintln("Failed to move element to new map", v, res))
					}
				}
				i++
			}

			//fmt.Printf("grew map from size %d (%d hashes) to %d (%d hashes)\n", len(m.bins), m.hashes, len(newm.bins), newm.hashes)
			//fmt.Println("reintegrated", i, "items; now updating map to", unsafe.Pointer(newm))
			atomic.StorePointer(&c.cmap, unsafe.Pointer(newm))
		}
		c.resize.Unlock()
		return c.op(key, upd)
	}

	return res
}

// Delete removes the value for the given key
func (c *Cuckoo) Delete(key []byte, casid uint64) MemopRes {
	c.resize.RLock()
	defer c.resize.RUnlock()
	return c.get().del(keyt(key), casid)
}

// Get returns the current value for the given key
func (c Cuckoo) Get(key []byte) (*Memval, bool) {
	v := c.get().get(keyt(key))
	if v.T == NOT_FOUND {
		return nil, false
	}
	return v.M, true
}

// Iterate returns a list of Memvals present in the map
func (c Cuckoo) Iterate() <-chan *Memval {
	ch := make(chan *Memval)
	go func() {
		for v := range c.get().iterate() {
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
		for v := range c.get().iterate() {
			ch <- string(v.key)
		}
		close(ch)
	}()
	return ch
}
