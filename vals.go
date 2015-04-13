package cuckoo

import "time"

// cval is a container for Cuckoo key data
type cval struct {
	bno int
	key keyt
	val Memval
}

// keyt is the internal representation of a map key
type keyt []byte

// present returns true if this key data has not yet expired
func (v *cval) present(now time.Time) bool {
	return v.val.Expires.IsZero() || v.val.Expires.After(now)
}
