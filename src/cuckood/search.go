package cuckoo

import (
	"bytes"
	"fmt"
	"time"
)

const MAX_SEARCH_DEPTH int = 1000

type mv struct {
	key  keyt
	from int
	to   int
	tobn int
}

func (m *cmap) search(now time.Time, bins ...int) []mv {
	for depth := 1; depth < MAX_SEARCH_DEPTH; depth++ {
		for _, b := range bins {
			path := m.find(nil, b, depth, now)
			if path != nil {
				return path
			}
		}
	}
	return nil
}

func (m *cmap) find(path []mv, bin int, depth int, now time.Time) []mv {
	if depth >= 0 {
		for i := 0; i < ASSOCIATIVITY; i++ {
			v := m.bins[bin].v(i)
			if v == nil || !v.present(now) {
				return path
			}

			path_ := make([]mv, len(path)+1)
			for i := range path {
				path_[i] = path[i]
			}

			from := bin
			to := from
			bno := v.bno + 1
			key := v.key
			for i := 0; i < int(m.hashes); i++ {
				bno = (bno + 1) % int(m.hashes)
				to = m.bin(bno, key)
				// XXX: could potentially try all bins here and
				// check each for available()? extra-broad
				// search...
				if to != from {
					break
				}
			}
			if to == from {
				continue
			}

			skip := false
			for _, p := range path {
				if p.from == to {
					skip = true
					break
				}
			}

			if skip {
				// XXX: could instead try next bin here
				continue
			}

			path_[len(path)] = mv{key, from, to, bno}
			if m.bins[to].available(now) {
				return path_
			} else {
				return m.find(path_, to, depth-1, now)
			}
		}
	}
	return nil
}

func (m *cmap) validate_execute(path []mv, now time.Time) bool {
	for i := len(path) - 1; i >= 0; i-- {
		k := path[i]

		m.lock_in_order(k.from, k.to)
		if !m.bins[k.to].available(now) {
			m.unlock(k.from, k.to)
			fmt.Println("path to occupancy no longer valid, target bucket now full")
			return false
		}

		ki := -1
		for j := 0; j < ASSOCIATIVITY; j++ {
			jk := m.bins[k.from].v(j)
			if jk != nil && jk.present(now) && bytes.Equal(jk.key, k.key) {
				ki = j
				break
			}
		}
		if ki == -1 {
			m.unlock(k.from, k.to)
			fmt.Println("path to occupancy no longer valid, key already swapped")
			return false
		}

		v := m.bins[k.from].v(ki)
		v.bno = k.tobn

		m.bins[k.to].subin(v, now)
		m.bins[k.from].kill(ki)

		m.unlock(k.from, k.to)
	}

	return true
}
