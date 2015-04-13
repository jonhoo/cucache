package cuckoo

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"
)

const ASSOCIATIVITY int = 8

type cmap struct {
	bins   []cbin
	hashes uint32
}

func (m *cmap) iterate() <-chan cval {
	now := time.Now()
	ch := make(chan cval)
	go func() {
		for i, bin := range m.bins {
			vals := make([]cval, 0, ASSOCIATIVITY)
			m.bins[i].mx.Lock()
			for vi := 0; vi < ASSOCIATIVITY; vi++ {
				v := bin.v(vi)
				if v != nil && v.present(now) {
					vals = append(vals, *v)
				}
			}
			m.bins[i].mx.Unlock()
			for _, cv := range vals {
				ch <- cv
			}
		}
		close(ch)
	}()
	return ch
}

// del removes the entry with the given key, and returns its value (if any)
func (m *cmap) del(key keyt) (ret MemopRes) {
	now := time.Now()
	bins := m.kbins(key)

	m.lock_in_order(bins...)
	defer m.unlock(bins...)

	ret.T = NOT_FOUND
	for _, bin := range bins {
		ki := m.bins[bin].has(key, now)
		if ki != -1 {
			ret.T = DELETED
			ret.V = &m.bins[bin].v(ki).val
			m.bins[bin].kill(ki)
			return
		}
	}
	return
}

func (m *cmap) insert(key keyt, upd Memop) (ret MemopRes) {
	now := time.Now()
	bins := m.kbins(key)

	ival := cval{key: key}

	m.lock_in_order(bins...)
	for bi, bin := range bins {
		b := &m.bins[bin]
		ki := b.has(key, now)
		if ki != -1 {
			ival.bno = bi
			ival.val, ret = upd(b.v(ki).val, true)
			if ret.T == STORED {
				b.setv(ki, &ival)
			}
			m.unlock(bins...)
			return
		}
	}
	m.unlock(bins...)

	for i, b := range bins {
		if m.bins[b].available(now) {
			ival.bno = i
			ret = m.bins[b].add(&ival, upd, now)
			if ret.T != SERVER_ERROR {
				return
			}
		}
	}

	for {
		path := m.search(now, bins...)
		if path == nil {
			return MemopRes{
				T: SERVER_ERROR,
				V: errors.New("no storage space found for element"),
			}
		}

		freeing := path[0].from

		// recompute bins because #hashes might have changed
		bins = m.kbins(key)

		// sanity check that this path will make room
		tobin := -1
		for i, bin := range bins {
			if freeing == bin {
				tobin = i
			}
		}
		if tobin == -1 {
			panic(fmt.Sprintf("path %v leads to occupancy in bin %v, but is unhelpful for key %s with bins: %v", path, freeing, key, bins))
		}

		if m.validate_execute(path, now) {
			ival.bno = tobin
			ret = m.bins[freeing].add(&ival, upd, now)
			if ret.T != SERVER_ERROR {
				return
			}
		}
	}
}

func (m *cmap) get(key keyt) (ret MemopRes) {
	now := time.Now()
	bins := m.kbins(key)

	ret.T = NOT_FOUND
	for _, bin := range bins {
		b := m.bins[bin]
		for i := 0; i < ASSOCIATIVITY; i++ {
			s := b.v(i)
			if s != nil && s.present(now) && bytes.Equal(s.key, key) {
				ret.T = EXISTS
				ret.V = &s.val
				return
			}
		}
	}
	return
}

func (m *cmap) lock_in_order(bins ...int) {
	locks := make([]int, len(bins))
	for i := range bins {
		locks[i] = bins[i]
	}

	sort.Ints(locks)
	last := -1
	for _, bin := range locks {
		if bin != last {
			m.bins[bin].mx.Lock()
			last = bin
		}
	}
}

func (m *cmap) unlock(bins ...int) {
	locks := make([]int, len(bins))
	for i := range bins {
		locks[i] = bins[i]
	}

	sort.Ints(locks)
	last := -1
	for _, bin := range locks {
		if bin != last {
			m.bins[bin].mx.Unlock()
			last = bin
		}
	}
}
