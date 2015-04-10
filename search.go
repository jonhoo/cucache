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
		for _, v := range m.bins[bin].vals {
			path_ := make([]mv, len(path)+1)
			for i := range path {
				path_[i] = path[i]
			}

			from := m.bin(v.bno, v.key)
			to := from
			bno := v.bno + 1
			for ; bno < int(m.hashes); bno++ {
				to = m.bin(bno, v.key)
				if to != from {
					break
				}
			}
			if to == from {
				continue
			}

			path_[len(path)] = mv{v.key, from, to, bno}
			if m.bins[to].available() {
				return path_
			} else {
				return m.find(path_, to, depth-1)
			}
		}
	}
	return nil
}
