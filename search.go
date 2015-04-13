package cuckoo

import "time"

const MAX_SEARCH_DEPTH int = 10

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
