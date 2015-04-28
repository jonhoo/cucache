package main

import (
	"bytes"
	"cuckood"
	"encoding/binary"
	"testing"

	gomem "github.com/dustin/gomemcached"
)

var noflag = []byte{0, 0, 0, 0}
var noexp = []byte{0, 0, 0, 0}
var nullset = []byte{0, 0, 0, 0, 0, 0, 0, 0}

func do(c *cuckoo.Cuckoo, t *testing.T, req *gomem.MCRequest) *gomem.MCResponse {
	res := req2res(c, req)
	if res.Status != gomem.SUCCESS {
		t.Errorf("expected operation %v to succeed; got %v", req, res.Status)
	}
	return res
}

func get(c *cuckoo.Cuckoo, key string) *gomem.MCResponse {
	return req2res(c, &gomem.MCRequest{
		Opcode: gomem.GET,
		Key:    []byte(key),
	})
}

func set(c *cuckoo.Cuckoo, key string, val []byte, as gomem.CommandCode) *gomem.MCResponse {
	return req2res(c, &gomem.MCRequest{
		Opcode: as,
		Key:    []byte(key),
		Body:   val,
		Extras: nullset,
	})
}

func pm(c *cuckoo.Cuckoo, key string, by uint64, def uint64, nocreate bool, as gomem.CommandCode) *gomem.MCResponse {
	extras := make([]byte, 20)
	binary.BigEndian.PutUint64(extras[0:8], by)
	binary.BigEndian.PutUint64(extras[8:16], def)
	binary.BigEndian.PutUint32(extras[16:20], 0)
	if nocreate {
		binary.BigEndian.PutUint32(extras[16:20], 0xffffffff)
	}
	return req2res(c, &gomem.MCRequest{
		Opcode: as,
		Key:    []byte(key),
		Extras: extras,
	})
}

func assertGet(c *cuckoo.Cuckoo, t *testing.T, key string, val []byte) *gomem.MCResponse {
	res := get(c, key)
	if res.Status != gomem.SUCCESS {
		t.Errorf("expected get success on key %s, got %v", key, res.Status)
	} else if !bytes.Equal(res.Body, val) {
		t.Errorf("expected get to return '%v' for key %s, got '%v'", string(val), key, string(res.Body))
	}
	return res
}

func assertNotExists(c *cuckoo.Cuckoo, t *testing.T, key string) {
	res := get(c, key)
	if res.Status != gomem.KEY_ENOENT {
		t.Errorf("expected get KEY_ENOENT on key %s, got %v", key, res.Status)
	}
	return
}

func assertSet(c *cuckoo.Cuckoo, t *testing.T, key string, val []byte, as gomem.CommandCode) *gomem.MCResponse {
	res := set(c, key, val, as)
	if res.Status != gomem.SUCCESS {
		t.Errorf("expected %v success for %s => %s, got %v", as, key, string(val), res.Status)
	}
	return res
}

func assertPM(c *cuckoo.Cuckoo, t *testing.T, key string, by uint64, def uint64, nocreate bool, as gomem.CommandCode) *gomem.MCResponse {
	res := pm(c, key, by, def, nocreate, as)
	if res.Status != gomem.SUCCESS {
		t.Errorf("expected success for %v(%d, %d, %v) on key %s, got %v", as, by, def, nocreate, key, res.Status)
	}
	return res
}
