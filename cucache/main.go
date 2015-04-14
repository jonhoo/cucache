package main

import (
	"bufio"
	"bytes"
	"cuckood"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	gomem "github.com/dustin/gomemcached"
)

var c cuckoo.Cuckoo

func main() {
	c = cuckoo.New()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ln, err := net.Listen("tcp", ":11211")
		if err != nil {
			panic(err)
		}
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println(err)
				continue
			}
			go handleConnection(conn)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ln, err := net.ListenPacket("udp", ":11211")
		if err != nil {
			panic(err)
		}
		for {
			b := make([]byte, 0, 10240)
			_, addr, err := ln.ReadFrom(b)
			if err != nil {
				fmt.Println(err)
				continue
			}
			go replyTo(b, addr.(*net.UDPAddr))
		}
	}()
	wg.Wait()
}

func wtf(req gomem.MCRequest, v cuckoo.MemopRes) {
	panic(fmt.Sprintf("unexpected result when handling %v: %v\n", req.Opcode, v))
}

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

func deal(in_ io.Reader, out io.Writer) {
	var getq []*gomem.MCResponse

	in := bufio.NewReader(in_)
	for {
		b, err := in.Peek(1)
		if err != nil {
			if err != io.EOF {
				// TODO print error
			}
			return
		}

		isbinary := true
		var req gomem.MCRequest
		var res gomem.MCResponse
		if b[0] == gomem.REQ_MAGIC {
			_, err := req.Receive(in, nil)
			if err != nil {
				// TODO: print error
				continue
			}
			res.Opaque = req.Opaque
		} else {
			// text protocol fallback
			cmd, err := in.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					// TODO: print error
				}
				return
			}

			req, err = Text2Req(cmd, in)
			isbinary = false
		}

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
				res.Cas = v.V.(uint64)
			case cuckoo.NOT_STORED:
				res.Status = gomem.NOT_STORED
			case cuckoo.NOT_FOUND:
				res.Status = gomem.KEY_ENOENT
			case cuckoo.EXISTS:
				res.Status = gomem.KEY_EEXISTS
			case cuckoo.SERVER_ERROR:
				res.Status = gomem.ENOMEM
				fmt.Println(v.V.(error))
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
				cv := v.V.(cuckoo.CasVal)
				res.Cas = cv.Casid
				res.Body = make([]byte, 8)
				binary.BigEndian.PutUint64(res.Body, cv.NewVal)
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
			// TODO: handle optional "now" argument
			// TODO: this is probably terrible
			c = cuckoo.New()
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

		if req.Opcode.IsQuiet() && res.Status == gomem.SUCCESS {
			if req.Opcode == gomem.GETQ || req.Opcode == gomem.GETKQ {
				getq = append(getq, &res)
				continue
			} else {
				continue
			}
		}

		if (req.Opcode == gomem.GETQ || req.Opcode == gomem.GETKQ) && res.Status == gomem.KEY_ENOENT {
			// no warning on cache miss
			continue
		}

		if res.Status != gomem.SUCCESS {
			fmt.Println(req.Opcode, res.Status)
		}

		if isbinary {
			// "The getq command is both mum on cache miss and quiet,
			// holding its response until a non-quiet command is issued."
			if !req.Opcode.IsQuiet() && len(getq) != 0 {
				// flush quieted get replies
				for _, r := range getq {
					r.Transmit(out)
				}
				getq = getq[:0]
			}

			res.Transmit(out)
			continue
		}

		if req.Opcode.IsQuiet() && res.Status == gomem.SUCCESS && req.Opcode != gomem.GET {
			// there is absolutely no reason to reply here
			continue
		}

		// TODO: return when writes fail
		switch res.Status {
		case gomem.SUCCESS:
			switch req.Opcode {
			case gomem.GET, gomem.GETQ:
				flags := binary.BigEndian.Uint32(res.Extras[0:4])
				out.Write([]byte(fmt.Sprintf("VALUE %s %d %d %d\r\n", req.Key, flags, len(res.Body), res.Cas)))
				out.Write(res.Body)
				out.Write([]byte{'\r', '\n'})
				out.Write([]byte("END\r\n"))
			case gomem.SET, gomem.ADD, gomem.REPLACE:
				out.Write([]byte("STORED\r\n"))
			case gomem.DELETE:
				out.Write([]byte("DELETED\r\n"))
			case gomem.INCREMENT, gomem.DECREMENT:
				v := binary.BigEndian.Uint64(res.Body)
				out.Write([]byte(strconv.FormatUint(v, 10) + "\r\n"))
			}
		case gomem.KEY_ENOENT:
			out.Write([]byte("NOT_FOUND\r\n"))
		case gomem.KEY_EEXISTS:
			out.Write([]byte("EXISTS\r\n"))
		case gomem.NOT_STORED:
			out.Write([]byte("NOT_STORED\r\n"))
		case gomem.ENOMEM:
			out.Write([]byte("SERVER_ERROR no space for new entry\r\n"))
		case gomem.DELTA_BADVAL:
			out.Write([]byte("CLIENT_ERROR incr/decr on non-numeric field\r\n"))
		case gomem.UNKNOWN_COMMAND:
			out.Write([]byte("ERROR\r\n"))
		}
	}
}

func replyTo(in []byte, to *net.UDPAddr) {
	u, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer u.Close()

	var o bytes.Buffer
	deal(bytes.NewBuffer(in), &o)
	_, err = u.WriteTo(o.Bytes(), to)
	if err != nil {
		fmt.Println(err)
	}
}

func handleConnection(c net.Conn) {
	deal(c, c)
	c.Close()
}
