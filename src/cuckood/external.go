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

// EVICTION_THRESHOLD sets a threshold for how many items must be evicted in
// the space of one second for a table resize to be performed.
const EVICTION_THRESHOLD = 1

// Cuckoo is an externally visible wrapper to a Cuckoo map implementation
type Cuckoo struct {
	resize sync.RWMutex
	cmap   unsafe.Pointer
	size   uint64
	tick   *time.Ticker
	done   chan struct{}
}

// New produces a new Cuckoo map. esize will be rounded to the next power of
// two. By default, 2 hashes are used. If esize is 0, the map will be
// initialized to hold 8192 elements. The map will automatically increase the
// number of hashes as the map fills to avoid spilling items.
func New(esize uint64) (c *Cuckoo) {
	if esize == 0 {
		esize = 1 << 16
	}

	c = &Cuckoo{
		sync.RWMutex{},
		unsafe.Pointer(create(esize)),
		esize,
		time.NewTicker(1 * time.Second),
		make(chan struct{}),
	}
	go c.fixer()
	return
}

func (c *Cuckoo) Capacity() uint64 {
	return atomic.LoadUint64(&c.size)
}

func (c *Cuckoo) Close() {
	close(c.done)

	c.resize.RLock()

	m := c.get()
	if m.requestEvict != nil {
		close(m.requestEvict)
	}
	atomic.StorePointer(&c.cmap, nil)
	atomic.StoreUint64(&c.size, 0)

	c.resize.RUnlock()
}

func (c *Cuckoo) EnableEviction() {
	c.resize.RLock()
	defer c.resize.RUnlock()
	c.get().enableEviction()
}

func (c Cuckoo) get() (m *cmap) {
	m = (*cmap)(atomic.LoadPointer(&c.cmap))
	if m == nil {
		panic("tried to use closed map")
	}
	return
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

// fix is called whenever an operation detects that the table is becoming
// crowded. this could be due to a large number of evictions, or because
// inserts start failing. fix will first attempt to increase the number of hash
// functions, and if that fails, it will resize the table.
func (c *Cuckoo) fix(nhashes uint32, oldm *cmap) {
	c.resize.RLock()

	m := c.get()
	if oldm != m {
		// someone else has resized the table
		c.resize.RUnlock()
		return
	}

	if nhashes+1 < MAX_HASHES {
		sw := atomic.CompareAndSwapUint32(&m.hashes, nhashes, nhashes+1)
		if sw {
			fmt.Fprintln(os.Stderr, "increased the number of hashes to", nhashes+1)
		}

		// regardless whether we succeeded or failed, the number of
		// hashes is now greater
		c.resize.RUnlock()
		return
	}

	c.resize.RUnlock()

	c.resize.Lock()
	defer c.resize.Unlock()
	if c.get() != m {
		// someone else already resized the map
		return
	}

	// note that there is no need to check #hashes, as it cannot have
	// increased beyong MAX_HASHES

	// if we get here, we're forced to resize the table
	// first, find new table size, and create it
	nsize := c.size << 1
	newm := create(nsize)
	fmt.Fprintln(os.Stderr, "growing hashtable to", nsize)

	// next, copy over all items
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
	}

	// stop eviction in old map
	// start eviction in new map
	if m.requestEvict != nil {
		close(m.requestEvict)
		newm.enableEviction()
	}

	// and finally, make the new map active
	atomic.StorePointer(&c.cmap, unsafe.Pointer(newm))
	atomic.StoreUint64(&c.size, nsize)
}

// fixer periodically checks how many evictions the table has seen, and will
// resize it if the number of evictions exceeds a threshold
func (c *Cuckoo) fixer() {
	var m *cmap
	var nh uint32

	var now uint
	var then uint
	var evicted uint
	for {
		select {
		case <-c.done:
			return
		case <-c.tick.C:
		}

		m = c.get()
		now = m.evicted
		nh = atomic.LoadUint32(&m.hashes)

		evicted = now - then
		if evicted > EVICTION_THRESHOLD {
			fmt.Fprintln(os.Stderr, "saw", evicted, "recent evictions; trying to fix")
			c.fix(nh, m)
		}

		then = now
	}
}

// op executes a particular Memop on the given key.
func (c *Cuckoo) op(key []byte, upd Memop) MemopRes {
	c.resize.RLock()
	m := c.get()
	nh := atomic.LoadUint32(&m.hashes)
	res := m.insert(keyt(key), upd)
	c.resize.RUnlock()

	for res.T == SERVER_ERROR {
		fmt.Fprintln(os.Stderr, "insert failed on key", string(key), "so trying to fix")
		c.fix(nh, m)

		c.resize.RLock()
		m = c.get()
		nh = atomic.LoadUint32(&m.hashes)
		res = m.insert(keyt(key), upd)
		c.resize.RUnlock()
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
