package text_test

import (
	"bytes"
	"cuckood/cucache/text"
	"encoding/binary"
	"fmt"
	"testing"

	gomem "github.com/dustin/gomemcached"
)

func hlp_out(t *testing.T, res gomem.MCResponse, exp string) {
	var out bytes.Buffer
	err := text.WriteMCResponse(&res, &out)
	if err != nil {
		t.Error(err)
		return
	}

	if !bytes.Equal(out.Bytes(), []byte(exp)) {
		t.Errorf("\nexpected:\n%+v\ngot:\n%+v", exp, out.String())
	}
}

func TestStorageOut(t *testing.T) {
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.SET,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "STORED\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.SET,
		Status: gomem.NOT_STORED,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "NOT_STORED\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.SET,
		Status: gomem.KEY_EEXISTS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "EXISTS\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.SET,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "NOT_FOUND\r\n")
}

func TestStorageOutQuiet(t *testing.T) {
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.SETQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.ADDQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.REPLACEQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.APPENDQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.PREPENDQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "")
}

func TestStorageOutQuietFail(t *testing.T) {
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.SETQ,
		Status: gomem.NOT_STORED,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "NOT_STORED\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.SETQ,
		Status: gomem.KEY_EEXISTS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "EXISTS\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.SETQ,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "NOT_FOUND\r\n")
}

func TestRetrievalOut(t *testing.T) {
	data := []byte("hello")
	flag := make([]byte, 4)
	binary.BigEndian.PutUint32(flag[0:4], 2)

	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.GETK,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    1,
		Extras: flag,
		Key:    []byte("a"),
		Body:   data,
		Fatal:  false,
	}, fmt.Sprintf("VALUE a 2 %d 1\r\n%s\r\nEND\r\n", len(data), data))
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.GETK,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: flag,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "END\r\n")
}

func TestMultiRetrievalOut(t *testing.T) {
	data := []byte("hello")
	flag := make([]byte, 4)
	binary.BigEndian.PutUint32(flag[0:4], 2)

	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.GETK,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    1,
		Extras: flag,
		Key:    []byte("a"),
		Body:   data,
		Fatal:  false,
	}, fmt.Sprintf("VALUE a 2 %d 1\r\n%s\r\nEND\r\n", len(data), data))
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.GETK,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: flag,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "END\r\n")

	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.GETKQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    1,
		Extras: flag,
		Key:    []byte("a"),
		Body:   data,
		Fatal:  false,
	}, fmt.Sprintf("VALUE a 2 %d 1\r\n%s\r\n", len(data), data))
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.GETKQ,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: flag,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "")
}

func TestDeletionOut(t *testing.T) {
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.DELETE,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "DELETED\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.DELETEQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.DELETE,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "NOT_FOUND\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.DELETEQ,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   nil,
		Fatal:  false,
	}, "NOT_FOUND\r\n")
}

func TestIncrementDecrementOut(t *testing.T) {
	newv := make([]byte, 8)
	binary.BigEndian.PutUint64(newv, 1)
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.INCREMENT,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   newv,
		Fatal:  false,
	}, "1\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.INCREMENTQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   newv,
		Fatal:  false,
	}, "")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.DECREMENT,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   newv,
		Fatal:  false,
	}, "1\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.DECREMENTQ,
		Status: gomem.SUCCESS,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   newv,
		Fatal:  false,
	}, "")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.INCREMENT,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   newv,
		Fatal:  false,
	}, "NOT_FOUND\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.INCREMENTQ,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   newv,
		Fatal:  false,
	}, "NOT_FOUND\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.DECREMENTQ,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   newv,
		Fatal:  false,
	}, "NOT_FOUND\r\n")
	hlp_out(t, gomem.MCResponse{
		Opcode: gomem.DECREMENT,
		Status: gomem.KEY_ENOENT,
		Opaque: 0,
		Cas:    0,
		Extras: nil,
		Key:    nil,
		Body:   newv,
		Fatal:  false,
	}, "NOT_FOUND\r\n")
}
