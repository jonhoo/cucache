package main

import (
	"bufio"
	"bytes"
	"cuckood"
	"cuckood/cucache/text"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"sync"
	"syscall"

	gomem "github.com/dustin/gomemcached"
)

var c cuckoo.Cuckoo

func main() {
	cpuprofile := flag.String("cpuprofile", "", "CPU profile output file")
	flag.Parse()

	c = cuckoo.New()

	var pf *os.File
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGABRT)
	go func() {
		for s := range sigs {
			if pf != nil {
				pprof.StopCPUProfile()
				err := pf.Close()
				if err != nil {
					fmt.Println("could not end cpu profile:", err)
				}
			}
			if s == os.Interrupt {
				os.Exit(0)
			}
		}
	}()

	var err error
	if cpuprofile != nil && *cpuprofile != "" {
		fmt.Println("starting CPU profiling")
		pf, err = os.Create(*cpuprofile)
		if err != nil {
			fmt.Printf("could not create CPU profile file %v: %v\n", *cpuprofile, err)
			return
		}
		err = pprof.StartCPUProfile(pf)
		if err != nil {
			fmt.Printf("could not start CPU profiling: %v\n", err)
			return
		}
	}

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

func wtf(req *gomem.MCRequest, v cuckoo.MemopRes) {
	panic(fmt.Sprintf("unexpected result when handling %v: %v\n", req.Opcode, v))
}

func execute(in <-chan *gomem.MCRequest, out chan<- *gomem.MCResponse) {
	mx := new(sync.Mutex)

	for req := range in {
		res := req2res(req)
		if req.Opcode.IsQuiet() && res.Status == gomem.SUCCESS {
			if req.Opcode == gomem.GETQ || req.Opcode == gomem.GETKQ {
				// simply don't flush
			} else {
				continue
			}
		}

		if (req.Opcode == gomem.GETQ || req.Opcode == gomem.GETKQ) && res.Status == gomem.KEY_ENOENT {
			// no warning on cache miss
			continue
		}

		if res.Status != gomem.SUCCESS {
			if !(res.Status == gomem.KEY_ENOENT && (req.Opcode == gomem.GET || req.Opcode == gomem.GETK)) {
				fmt.Println(req.Opcode, res.Status)
			}
		}

		mx.Lock()
		go func() {
			out <- res
			mx.Unlock()
		}()
	}
	close(out)
}

func writeback(in <-chan *gomem.MCResponse, out_ io.Writer) {
	out := bufio.NewWriter(out_)
	mx := new(sync.Mutex)

	for res := range in {
		if res.Opaque != 0xffffffff {
			// binary protocol
			b := res.Bytes()

			mx.Lock()
			out.Write(b)

			// "The getq command is both mum on cache miss and quiet,
			// holding its response until a non-quiet command is issued."
			if res.Opcode.IsQuiet() == false {
				// This allows us to do Bytes() and Flush() in
				// parallel
				go func() {
					out.Flush()
					mx.Unlock()
				}()
			} else {
				mx.Unlock()
			}
			continue
		}

		// we've got a text protocol client
		if res.Opcode.IsQuiet() && res.Status == gomem.SUCCESS {
			// there is absolutely no reason to reply here
			// a noreply get doesn't exist in the text protocol
			continue
		}

		// TODO: return when writes fail
		switch res.Status {
		case gomem.SUCCESS:
			switch res.Opcode {
			case gomem.GETK:
				flags := binary.BigEndian.Uint32(res.Extras[0:4])
				out.Write([]byte(fmt.Sprintf("VALUE %s %d %d %d\r\n", res.Key, flags, len(res.Body), res.Cas)))
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
		resP.Put(res)
	}
}

func parse(in_ io.Reader, out chan<- *gomem.MCRequest) {
	in := bufio.NewReader(in_)

	for {
		b, err := in.Peek(1)
		if err != nil {
			if err == io.EOF {
				return
			}
			// TODO print error
			return
		}

		req := new(gomem.MCRequest)
		if b[0] == gomem.REQ_MAGIC {
			_, err := req.Receive(in, nil)
			if err != nil {
				if err == io.EOF {
					return
				}
				// TODO: print error
				continue
			}
		} else {
			// text protocol fallback
			cmd, err := in.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					return
				}
				// TODO: print error
				return
			}

			*req, err = text.ToMCRequest(cmd, in)
			req.Opaque = 0xffffffff
		}

		out <- req
	}
	close(out)
}

func setup(in io.Reader, out io.Writer) {
	dispatch := make(chan *gomem.MCRequest, 50)
	bridge := make(chan *gomem.MCResponse, 50)
	go execute(dispatch, bridge)
	go writeback(bridge, out)
	parse(in, dispatch)
}

func replyTo(in []byte, to *net.UDPAddr) {
	u, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer u.Close()

	var o bytes.Buffer
	setup(bytes.NewBuffer(in), &o)
	_, err = u.WriteTo(o.Bytes(), to)
	if err != nil {
		fmt.Println(err)
	}
}

func handleConnection(c net.Conn) {
	setup(c, c)
	c.Close()
}
