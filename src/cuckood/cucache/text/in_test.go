package text_test

import (
	"bytes"
	"cuckood/cucache/text"
	"encoding/binary"
	"fmt"
	"testing"

	gomem "github.com/dustin/gomemcached"
)

func hlp(t *testing.T, cmd string, in []byte, exp gomem.MCRequest) {
	hlps(t, cmd, in, []gomem.MCRequest{exp})
}

func hlps(t *testing.T, cmd string, in []byte, exp []gomem.MCRequest) {
	var in_ bytes.Buffer
	in_.Write(in)

	reqs, err := text.ToMCRequest(cmd, &in_)
	if err != nil {
		t.Error(err)
		return
	}

	if len(exp) != len(reqs) {
		t.Errorf("expected %d request objects, got %d\n", len(exp), len(reqs))
	}

	for i := range exp {
		if !bytes.Equal(reqs[i].Bytes(), exp[i].Bytes()) {
			t.Errorf("\n[%d] expected:\n%+v\ngot:\n%+v", i, &exp[i], reqs[i])
		}
	}
}

func TestStorage(t *testing.T) {
	bits := []byte("value")
	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras[0:4], 1)
	binary.BigEndian.PutUint32(extras[4:8], 2)

	hlp(t, fmt.Sprintf("set a 1 2 %d", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.SET,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("cas a 1 2 %d 3", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.SET,
		Cas:    3,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("add a 1 2 %d", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.ADD,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("replace a 1 2 %d", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.REPLACE,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("append a %d", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.APPEND,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("prepend a %d", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.PREPEND,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   bits,
	})
}

func TestStorageQuiet(t *testing.T) {
	bits := []byte("value")
	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras[0:4], 1)
	binary.BigEndian.PutUint32(extras[4:8], 2)

	hlp(t, fmt.Sprintf("set a 1 2 %d noreply", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.SETQ,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("cas a 1 2 %d 3 noreply", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.SETQ,
		Cas:    3,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("add a 1 2 %d noreply", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.ADDQ,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("replace a 1 2 %d noreply", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.REPLACEQ,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("append a %d noreply", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.APPENDQ,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   bits,
	})
	hlp(t, fmt.Sprintf("prepend a %d noreply", len(bits)), []byte(string(bits)+"\r\n"), gomem.MCRequest{
		Opcode: gomem.PREPENDQ,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   bits,
	})
}

func TestRetrieval(t *testing.T) {
	hlp(t, "get a", nil, gomem.MCRequest{
		Opcode: gomem.GETK,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   nil,
	})
	hlp(t, "gets a", nil, gomem.MCRequest{
		Opcode: gomem.GETK,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   nil,
	})
}

func TestMultiRetrieval(t *testing.T) {
	key_a := gomem.MCRequest{
		Opcode: gomem.GETKQ,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   nil,
	}

	key_b := key_a
	key_b.Opcode = gomem.GETK
	key_b.Key = []byte("b")

	hlps(t, "get a b", nil, []gomem.MCRequest{key_a, key_b})
	hlps(t, "gets a b", nil, []gomem.MCRequest{key_a, key_b})
}

func TestDeletion(t *testing.T) {
	hlp(t, "delete a", nil, gomem.MCRequest{
		Opcode: gomem.DELETE,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   nil,
	})
	hlp(t, "delete a noreply", nil, gomem.MCRequest{
		Opcode: gomem.DELETEQ,
		Cas:    0,
		Opaque: 0,
		Extras: nil,
		Key:    []byte("a"),
		Body:   nil,
	})
}

func TestIncrementDecrement(t *testing.T) {
	extras := make([]byte, 20)
	binary.BigEndian.PutUint64(extras[0:8], 1)
	binary.BigEndian.PutUint64(extras[8:16], 0)
	binary.BigEndian.PutUint32(extras[16:20], 0xffffffff)

	hlp(t, "incr a 1", nil, gomem.MCRequest{
		Opcode: gomem.INCREMENT,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   nil,
	})
	hlp(t, "decr a 1", nil, gomem.MCRequest{
		Opcode: gomem.DECREMENT,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   nil,
	})
}

func TestIncrementDecrementQuiet(t *testing.T) {
	extras := make([]byte, 20)
	binary.BigEndian.PutUint64(extras[0:8], 1)
	binary.BigEndian.PutUint64(extras[8:16], 0)
	binary.BigEndian.PutUint32(extras[16:20], 0xffffffff)

	hlp(t, "incr a 1 noreply", nil, gomem.MCRequest{
		Opcode: gomem.INCREMENTQ,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   nil,
	})
	hlp(t, "decr a 1 noreply", nil, gomem.MCRequest{
		Opcode: gomem.DECREMENTQ,
		Cas:    0,
		Opaque: 0,
		Extras: extras,
		Key:    []byte("a"),
		Body:   nil,
	})
}

// TODO: TestTouch
