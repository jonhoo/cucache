package cuckoo

import "time"

type cval struct {
	bno int
	key keyt
	val Memval
}

func (v *cval) present(now time.Time) bool {
	return v.val.Expires.IsZero() || v.val.Expires.After(now)
}
