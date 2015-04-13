package cuckoo

import (
	"errors"
	"fmt"
	"sync/atomic"
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

func (c Cuckoo) Insert(key string, value interface{}) error {
	h := c.hashes
	pathl := c.insert(keyt(key), value)

	for pathl == -1 && h < MAX_HASHES {
		sw := atomic.CompareAndSwapUint32(&c.hashes, h, h+1)
		if sw {
			fmt.Println("insert failed on key", key, ", so upped # hashes to", h+1)
		}

		h = c.hashes
		pathl = c.insert(keyt(key), value)
	}

	if pathl == -1 {
		return errors.New("insert failed, table must be full (or have bad cycles)")
	}
	return nil
}

func (c Cuckoo) Get(key string) (interface{}, bool) {
	return c.get(keyt(key))
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
