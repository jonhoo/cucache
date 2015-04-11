package cuckoo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
)

const ASSOCIATIVITY int = 8

func (m *cmap) bin(n int, key keyt) int {
	h := fnv.New64a()
	h.Write(key)

	var bs [8]byte
	binary.PutVarint(bs[:], int64(n))
	h.Write(bs[:])

	return int(h.Sum64() % uint64(len(m.bins)))
}

func (m *cmap) kbins(key keyt) []int {
	bins := make([]int, 0, m.hashes)
	h := fnv.New64a()
	for i := 0; i < int(m.hashes); i++ {
		h.Reset()
		h.Write(key)

		var bs [8]byte
		binary.PutVarint(bs[:], int64(i))
		h.Write(bs[:])

		bins = append(bins, int(h.Sum64()%uint64(len(m.bins))))
	}
	return bins
}

type cval struct {
	bno     int
	expires int
	key     keyt
	val     interface{}
}

func (v *cval) present() bool {
	return !(v.expires == 0 || v.val == nil)
}

type cbin struct {
	vals [ASSOCIATIVITY]cval
	mx   SpinLock
}

func (b *cbin) subin(v cval) {
	for j, jv := range b.vals {
		if !jv.present() {
			b.vals[j] = v
		}
	}
}

func (b *cbin) kill(i int) {
	b.vals[i].expires = 0
}

func (b *cbin) available() bool {
	present := 0
	for i := 0; i < ASSOCIATIVITY; i++ {
		if b.vals[i].present() {
			present++
		}
	}
	return present < ASSOCIATIVITY
}

type cmap struct {
	bins   []cbin
	hashes uint32
}

func (m *cmap) iterate() <-chan cval {
	ch := make(chan cval)
	go func() {
		for i, bin := range m.bins {
			vals := make([]cval, 0, ASSOCIATIVITY)
			m.bins[i].mx.Lock()
			for _, v := range bin.vals {
				if v.present() {
					vals = append(vals, v)
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

func (m *cmap) add(bini int, bin int, key keyt, val interface{}) bool {
	m.bins[bin].mx.Lock()
	defer m.bins[bin].mx.Unlock()
	if m.bins[bin].available() {
		m.bins[bin].subin(cval{bini, 1, key, val})
		return true
	}
	return false
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

func (m *cmap) validate_execute(path []mv) bool {
	for i := len(path) - 1; i >= 0; i-- {
		k := path[i]

		m.lock_in_order(k.from, k.to)
		if !m.bins[k.to].available() {
			m.unlock(k.from, k.to)
			fmt.Println("path to occupancy no longer valid, target bucket now full")
			return false
		}

		ki := -1
		for j, jk := range m.bins[k.from].vals {
			if jk.present() && bytes.Equal(jk.key, k.key) {
				ki = j
				break
			}
		}
		if ki == -1 {
			m.unlock(k.from, k.to)
			fmt.Println("path to occupancy no longer valid, key already swapped")
			return false
		}

		v := m.bins[k.from].vals[ki]
		v.bno = k.tobn

		m.bins[k.to].subin(v)
		m.bins[k.from].kill(ki)

		m.unlock(k.from, k.to)
	}

	return true
}

func (m *cmap) has(bin int, key keyt) int {
	for i := 0; i < ASSOCIATIVITY; i++ {
		if m.bins[bin].vals[i].present() && bytes.Equal(m.bins[bin].vals[i].key, key) {
			return i
		}
	}
	return -1
}

// del removes the entry with the given key, and returns its value (if any)
func (m *cmap) del(key keyt) (v interface{}) {
	bins := m.kbins(key)

	m.lock_in_order(bins...)
	defer m.unlock(bins...)

	for _, bin := range bins {
		ki := m.has(bin, key)
		if ki != -1 {
			v = m.bins[bin].vals[ki]
			m.bins[bin].kill(ki)
			return
		}
	}
	return nil
}

func (m *cmap) insert(key keyt, val interface{}) int {
	bins := m.kbins(key)

	m.lock_in_order(bins...)
	for _, bin := range bins {
		ki := m.has(bin, key)
		if ki != -1 {
			m.bins[bin].vals[ki].val = val
			m.unlock(bins...)
			return 0
		}
	}
	m.unlock(bins...)

	for i, b := range bins {
		if m.bins[b].available() {
			if m.add(i, b, key, val) {
				return 0
			}
		}
	}

	for {
		path := m.search(bins...)
		if path == nil {
			return -1
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

		if m.validate_execute(path) {
			if m.add(tobin, freeing, key, val) {
				return len(path)
			}
		}
	}
}

func (m *cmap) get(key keyt) (interface{}, bool) {
	bins := m.kbins(key)

	for _, bin := range bins {
		b := m.bins[bin]
		for _, s := range b.vals {
			if s.present() && bytes.Equal(s.key, key) {
				return s.val, true
			}
		}
	}
	return nil, false
}
