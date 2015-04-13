package cuckoo

import (
	"fmt"
	"sync/atomic"
	"time"
)

const MAX_HASHES = 10

type keyt []byte

type Cuckoo struct{ *cmap }

func New() Cuckoo {
	m := new(cmap)
	m.bins = make([]cbin, 1e4 /* TODO: all of main memory */)
	m.hashes = 2
	return Cuckoo{m}
}

func (c Cuckoo) Set(key string, bytes []byte, flags uint16, expires time.Time) MemopRes {
	return c.op(key, fset(bytes, flags, expires))
}
func (c Cuckoo) Add(key string, bytes []byte, flags uint16, expires time.Time) MemopRes {
	return c.op(key, fadd(bytes, flags, expires))
}
func (c Cuckoo) Replace(key string, bytes []byte, flags uint16, expires time.Time) MemopRes {
	return c.op(key, freplace(bytes, flags, expires))
}
func (c Cuckoo) Append(key string, bytes []byte) MemopRes {
	return c.op(key, fappend(bytes))
}
func (c Cuckoo) Prepent(key string, bytes []byte) MemopRes {
	return c.op(key, fprepend(bytes))
}
func (c Cuckoo) CAS(key string, bytes []byte, flags uint16, expires time.Time, casid uint64) MemopRes {
	return c.op(key, fcas(bytes, flags, expires, casid))
}
func (c Cuckoo) Incr(key string, by uint64) MemopRes {
	return c.op(key, fincr(by))
}
func (c Cuckoo) Decr(key string, by uint64) MemopRes {
	return c.op(key, fdecr(by))
}

func (c Cuckoo) op(key string, upd Memop) MemopRes {
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

func (c Cuckoo) Delete(key string) MemopResType {
	return c.del(keyt(key)).T
}

func (c Cuckoo) Get(key string) (*Memval, bool) {
	v := c.get(keyt(key))
	if v.T == NOT_FOUND {
		return nil, false
	}
	return v.V.(*Memval), true
}

func (c Cuckoo) Iterate() <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		for v := range c.iterate() {
			ch <- v.val
		}
		close(ch)
	}()
	return ch
}

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
