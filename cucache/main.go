package main

import (
	"bufio"
	"bytes"
	"cuckood"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
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

func check_args(o io.Writer, args []string, argv int) bool {
	if len(args) != argv {
		o.Write([]byte(fmt.Sprintf("CLIENT_ERROR wrong number of arguments (got %d, expected %d)\r\n", len(args), argv)))
		return false
	}
	return true
}

func data(lenarg string, in *bufio.Reader) ([]byte, error) {
	ln, err := strconv.Atoi(lenarg)
	if err != nil {
		return nil, err
	}

	b := make([]byte, ln)
	_, err = io.ReadFull(in, b)
	if err == nil {
		// remove \r\n
		r, err := in.ReadByte()
		if err != nil {
			return nil, err
		}
		n, err := in.ReadByte()
		if err != nil {
			return nil, err
		}
		if r != '\r' || n != '\n' {
			return nil, errors.New("data not terminated by \\r\\n")
		}
	}
	return b, err
}

func cerr(out io.Writer, err error) {
	out.Write([]byte("CLIENT_ERROR " + err.Error() + "\r\n"))
}

func serr(out io.Writer, err error) {
	out.Write([]byte("SERVER_ERROR " + err.Error() + "\r\n"))
}

func tm(in string) (t time.Time, err error) {
	var i int
	i, err = strconv.Atoi(in)
	if err != nil {
		return
	}

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

func end(v cuckoo.MemopRes, out io.Writer) {
	if v.T == cuckoo.SERVER_ERROR {
		serr(out, v.V.(error))
		return
	}
	out.Write([]byte(v.T.String() + "\r\n"))
}

func setargs(args []string, in *bufio.Reader) (flags uint16, exp time.Time, value []byte, err error) {
	nbytes := args[len(args)-1]
	args = args[:len(args)-1]
	if len(args) >= 1 {
		var flags_ uint64
		flags_, err = strconv.ParseUint(args[0], 10, 16)
		if err != nil {
			return
		}
		flags = uint16(flags_)
	}

	if len(args) >= 2 {
		exp, err = tm(args[1])
		if err != nil {
			return
		}
	}

	value, err = data(nbytes, in)
	return
}

func deal(in_ io.Reader, out io.Writer) {
	in := bufio.NewReader(in_)
	for {
		// TODO: return when writes fail
		cmd, err := in.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				cerr(out, err)
			}
			return
		}

		args := strings.Fields(strings.TrimSpace(cmd))
		cmd = args[0]
		args = args[1:]

		fmt.Println("got cmd", cmd, "with args", args)

		isget := strings.HasPrefix(cmd, "get")
		if !isget && len(args) != 0 && args[len(args)-1] == "noreply" {
			out = ioutil.Discard
			args = args[:len(args)-1]
		}

		switch cmd {
		case "set", "add", "replace", "append", "prepend":
			var nargs int
			switch cmd {
			case "append", "prepend":
				nargs = 2
			case "cas":
				nargs = 5
			default:
				nargs = 4
			}

			if !check_args(out, args, nargs) {
				continue
			}

			var casid uint64
			if cmd == "cas" {
				casid, err = strconv.ParseUint(args[len(args)-1], 10, 64)
				if err != nil {
					cerr(out, err)
					continue
				}
				args = args[:len(args)-1]
			}

			key := args[0]
			args = args[1:]
			flags, exp, val, err := setargs(args, in)
			if err != nil {
				cerr(out, err)
				continue
			}

			var v cuckoo.MemopRes
			switch cmd {
			case "set":
				v = c.Set(key, val, uint16(flags), exp)
			case "add":
				v = c.Add(key, val, uint16(flags), exp)
			case "replace":
				v = c.Replace(key, val, uint16(flags), exp)
			case "append":
				v = c.Append(key, val)
			case "prepend":
				v = c.Prepend(key, val)
			case "cas":
				v = c.CAS(key, val, uint16(flags), exp, casid)
			}

			end(v, out)
		case "incr", "decr":
			if !check_args(out, args, 2) {
				continue
			}

			by, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				cerr(out, err)
				continue
			}

			var v cuckoo.MemopRes
			if cmd == "incr" {
				v = c.Incr(args[0], by)
			} else {
				v = c.Decr(args[0], by)
			}

			if v.T == cuckoo.STORED {
				out.Write([]byte(strconv.FormatUint(v.V.(uint64), 10) + "\r\n"))
			} else {
				end(v, out)
			}
		case "touch":
			if !check_args(out, args, 2) {
				continue
			}

			exp, err := tm(args[1])
			if err != nil {
				cerr(out, err)
				continue
			}

			v := c.Touch(args[0], exp)
			if v.T == cuckoo.STORED {
				out.Write([]byte("TOUCHED\r\n"))
			} else {
				end(v, out)
			}
		case "delete":
			if !check_args(out, args, 1) {
				continue
			}

			v := c.Delete(args[0])
			if v.T == cuckoo.STORED {
				out.Write([]byte("DELETED\r\n"))
			} else {
				end(v, out)
			}
		case "get", "gets":
			res := make(map[string]*cuckoo.Memval)

			var mx sync.Mutex
			var wg sync.WaitGroup
			for _, key := range args {
				wg.Add(1)
				go func(key string) {
					defer wg.Done()
					v, ok := c.Get(key)
					if ok {
						mx.Lock()
						res[key] = v
						mx.Unlock()
					}
				}(key)
			}
			wg.Wait()

			var err error
			for _, key := range args {
				v, ok := res[key]
				if ok {
					_, err = out.Write([]byte(fmt.Sprintf("VALUE %s %d %d %d\r\n", key, v.Flags, len(v.Bytes), v.Casid)))
					if err != nil {
						return
					}
					_, err = out.Write(v.Bytes)
					if err != nil {
						return
					}
					out.Write([]byte{'\r', '\n'})
				}
			}
			out.Write([]byte("END\r\n"))
		case "quit":
			return
		case "flush_all":
			// TODO: this is probably terrible
			c = cuckoo.New()
			out.Write([]byte("OK\r\n"))
		default:
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
	fmt.Println("UDP deal")
	deal(bytes.NewBuffer(in), &o)
	fmt.Println("UDP done deal")
	_, err = u.WriteTo(o.Bytes(), to)
	if err != nil {
		fmt.Println(err)
	}
}

func handleConnection(c net.Conn) {
	fmt.Println("TCP deal")
	deal(c, c)
	fmt.Println("TCP done deal")
	c.Close()
}
