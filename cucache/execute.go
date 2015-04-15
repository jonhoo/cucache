package main

import (
	"cuckood"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	gomem "github.com/dustin/gomemcached"
)

func tm(i uint32) (t time.Time) {
	if i == 0 {
		return
	}

	if i < 60*60*24*30 {
		t = time.Now().Add(time.Duration(i) * time.Second)
	} else {
		t = time.Unix(int64(i), 0)
	}
	return
}

func req2res(req *gomem.MCRequest) (res *gomem.MCResponse) {
	res = resP.Get().(*gomem.MCResponse)
	res.Cas = 0
	res.Key = nil
	res.Body = nil
	res.Extras = nil
	res.Fatal = false
	res.Status = 0

	res.Opaque = req.Opaque
	res.Opcode = req.Opcode

	switch req.Opcode {
	case gomem.GET, gomem.GETQ, gomem.GETK, gomem.GETKQ:
		res.Status = gomem.KEY_ENOENT
		v, ok := c.Get(req.Key)
		if ok {
			res.Status = gomem.SUCCESS
			res.Extras = make([]byte, 4)
			binary.BigEndian.PutUint32(res.Extras, v.Flags)
			res.Cas = v.Casid
			res.Body = v.Bytes

			if req.Opcode == gomem.GETK || req.Opcode == gomem.GETKQ {
				res.Key = req.Key
			}
		}
	case gomem.SET, gomem.SETQ,
		gomem.ADD, gomem.ADDQ,
		gomem.REPLACE, gomem.REPLACEQ:

		flags := binary.BigEndian.Uint32(req.Extras[0:4])
		expiry := tm(binary.BigEndian.Uint32(req.Extras[4:8]))
		var v cuckoo.MemopRes
		switch req.Opcode {
		case gomem.SET, gomem.SETQ:
			if req.Cas == 0 {
				v = c.Set(req.Key, req.Body, flags, expiry)
			} else {
				v = c.CAS(req.Key, req.Body, flags, expiry, req.Cas)
			}
		case gomem.ADD, gomem.ADDQ:
			v = c.Add(req.Key, req.Body, flags, expiry)
		case gomem.REPLACE, gomem.REPLACEQ:
			if req.Cas == 0 {
				v = c.Replace(req.Key, req.Body, flags, expiry)
			} else {
				v = c.CAS(req.Key, req.Body, flags, expiry, req.Cas)
			}
		}

		switch v.T {
		case cuckoo.STORED:
			res.Status = gomem.SUCCESS
			res.Cas = v.M.Casid
		case cuckoo.NOT_STORED:
			res.Status = gomem.NOT_STORED
		case cuckoo.NOT_FOUND:
			res.Status = gomem.KEY_ENOENT
		case cuckoo.EXISTS:
			res.Status = gomem.KEY_EEXISTS
		case cuckoo.SERVER_ERROR:
			res.Status = gomem.ENOMEM
			fmt.Println(v.E)
		default:
			wtf(req, v)
		}
	case gomem.DELETE, gomem.DELETEQ:
		v := c.Delete(req.Key, req.Cas)

		switch v.T {
		case cuckoo.STORED:
			res.Status = gomem.SUCCESS
		case cuckoo.NOT_FOUND:
			res.Status = gomem.KEY_ENOENT
		case cuckoo.EXISTS:
			res.Status = gomem.KEY_EEXISTS
		default:
			wtf(req, v)
		}
	case gomem.INCREMENT, gomem.INCREMENTQ,
		gomem.DECREMENT, gomem.DECREMENTQ:

		by := binary.BigEndian.Uint64(req.Extras[0:8])
		def := binary.BigEndian.Uint64(req.Extras[8:16])
		exp := tm(binary.BigEndian.Uint32(req.Extras[16:20]))

		if binary.BigEndian.Uint32(req.Extras[16:20]) == 0xffffffff {
			exp = time.Unix(math.MaxInt64, 0)
		}

		var v cuckoo.MemopRes
		if req.Opcode == gomem.INCREMENT || req.Opcode == gomem.INCREMENTQ {
			v = c.Incr(req.Key, by, def, exp)
		} else {
			v = c.Decr(req.Key, by, def, exp)
		}

		switch v.T {
		case cuckoo.STORED:
			res.Status = gomem.SUCCESS
			res.Cas = v.M.Casid
			newVal, _ := strconv.ParseUint(string(v.M.Bytes), 10, 64)
			res.Body = make([]byte, 8)
			binary.BigEndian.PutUint64(res.Body, newVal)
		case cuckoo.CLIENT_ERROR:
			res.Status = gomem.DELTA_BADVAL
		case cuckoo.NOT_FOUND:
			res.Status = gomem.KEY_ENOENT
		default:
			wtf(req, v)
		}
	case gomem.QUIT, gomem.QUITQ:
		return
	case gomem.FLUSH, gomem.FLUSHQ:
		at := tm(binary.BigEndian.Uint32(req.Extras[0:4]))
		if at.IsZero() {
			at = time.Now()
		}
		c.TouchAll(at)
		res.Status = gomem.SUCCESS
	case gomem.NOOP:
		res.Status = gomem.SUCCESS
	case gomem.VERSION:
		res.Status = gomem.SUCCESS
		// TODO: res.Body =
	case gomem.APPEND, gomem.APPENDQ,
		gomem.PREPEND, gomem.PREPENDQ:

		var v cuckoo.MemopRes
		switch req.Opcode {
		case gomem.APPEND, gomem.APPENDQ:
			v = c.Append(req.Key, req.Body, req.Cas)
		case gomem.PREPEND, gomem.PREPENDQ:
			v = c.Prepend(req.Key, req.Body, req.Cas)
		}

		switch v.T {
		case cuckoo.STORED:
			res.Status = gomem.SUCCESS
		case cuckoo.EXISTS:
			res.Status = gomem.KEY_EEXISTS
		case cuckoo.NOT_FOUND:
			res.Status = gomem.KEY_ENOENT
		default:
			wtf(req, v)
		}
	default:
		res.Status = gomem.UNKNOWN_COMMAND
	}

	switch res.Status {
	case gomem.KEY_ENOENT:
		res.Body = []byte("Not found")
	case gomem.KEY_EEXISTS:
		res.Body = []byte("Data exists for key.")
	case gomem.NOT_STORED:
		res.Body = []byte("Not stored.")
	case gomem.ENOMEM:
		res.Body = []byte("Out of memory")
	case gomem.UNKNOWN_COMMAND:
		res.Body = []byte("Unknown command")
	case gomem.EINVAL:
		res.Body = []byte("Invalid arguments")
	case gomem.E2BIG:
		res.Body = []byte("Too large.")
	case gomem.DELTA_BADVAL:
		res.Body = []byte("Non-numeric server-side value for incr or decr")
	}

	return
}
