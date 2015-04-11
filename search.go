package cuckoo

const MAX_SEARCH_DEPTH int = 10

type mv struct {
	key  keyt
	from int
	to   int
	tobn int
}

func (m *cmap) search(bins ...int) []mv {
	for depth := 1; depth < MAX_SEARCH_DEPTH; depth++ {
		for _, b := range bins {
			path := m.find(nil, b, depth)
			if path != nil {
				return path
			}
		}
	}
	return nil
}

func (m *cmap) find(path []mv, bin int, depth int) []mv {
	if depth >= 0 {
		for i := 0; i < ASSOCIATIVITY; i++ {
			if !m.bins[bin].vals[i].present() {
				return path
			}

			path_ := make([]mv, len(path)+1)
			for i := range path {
				path_[i] = path[i]
			}

			from := bin
			to := from
			bno := m.bins[bin].vals[i].bno + 1
			key := m.bins[bin].vals[i].key
			for ; bno < int(m.hashes); bno++ {
				to = m.bin(bno, key)
				if to != from {
					break
				}
			}
			if to == from {
				continue
			}

			path_[len(path)] = mv{key, from, to, bno}
			if m.bins[to].available() {
				return path_
			} else {
				return m.find(path_, to, depth-1)
			}
		}
	}
	return nil
}
