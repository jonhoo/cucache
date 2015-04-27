package main

import (
	"bufio"
	"bytes"
	"cuckood"
	"cuckood/cucache/text"
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

var reqP sync.Pool
var resP sync.Pool

func init() {
	reqP.New = func() interface{} {
		return new(gomem.MCRequest)
	}
	resP.New = func() interface{} {
		return new(gomem.MCResponse)
	}
}

var not_found []byte

func main() {
	cpuprofile := flag.String("cpuprofile", "", "CPU profile output file")
	port := flag.Int("p", 11211, "TCP port to listen on")
	udpport := flag.Int("U", 11211, "UDP port to listen on")
	flag.Parse()

	c := cuckoo.New(1e5)

	var pf *os.File
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGABRT)
	go func() {
		for range sigs {
			if pf != nil {
				pprof.StopCPUProfile()
				err := pf.Close()
				if err != nil {
					fmt.Println("could not end cpu profile:", err)
				}
			}
			os.Exit(0)
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

	not_found = req2res(c, &gomem.MCRequest{
		Opcode: gomem.GET,
		Key:    []byte("there is no key"),
	}).Bytes()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
		if err != nil {
			panic(err)
		}
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println(err)
				continue
			}
			go handleConnection(c, conn)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ln, err := net.ListenPacket("udp", ":"+strconv.Itoa(*udpport))
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
			go replyTo(c, b, addr.(*net.UDPAddr))
		}
	}()
	wg.Wait()
}

func wtf(req *gomem.MCRequest, v cuckoo.MemopRes) {
	panic(fmt.Sprintf("unexpected result when handling %v: %v\n", req.Opcode, v))
}

func execute(c cuckoo.Cuckoo, in <-chan *gomem.MCRequest, out chan<- *gomem.MCResponse) {
	mx := new(sync.Mutex)

	for req := range in {
		res := req2res(c, req)
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

		reqP.Put(req)
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
			var b []byte
			quiet := res.Opcode.IsQuiet()
			if res.Opcode == gomem.GET && res.Opaque == 0 && res.Status == gomem.KEY_ENOENT {
				b = not_found
			} else {
				b = res.Bytes()
			}
			resP.Put(res)

			mx.Lock()
			out.Write(b)

			// "The getq command is both mum on cache miss and quiet,
			// holding its response until a non-quiet command is issued."
			if !quiet {
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
		err := text.WriteMCResponse(res, out)
		_ = err // TODO
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

		if b[0] == gomem.REQ_MAGIC {
			req := reqP.Get().(*gomem.MCRequest)
			req.Cas = 0
			req.Key = nil
			req.Body = nil
			req.Extras = nil
			req.Opcode = 0
			req.Opaque = 0
			_, err := req.Receive(in, nil)
			if err != nil {
				if err == io.EOF {
					reqP.Put(req)
					return
				}
				// TODO: print error
				continue
			}
			out <- req
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

			reqs, err := text.ToMCRequest(cmd, in)
			if err != nil {
				// TODO: print error
				return
			}
			for _, req := range reqs {
				req.Opaque = 0xffffffff
				out <- &req
			}
		}
	}
	close(out)
}

func setup(c cuckoo.Cuckoo, in io.Reader, out io.Writer) {
	dispatch := make(chan *gomem.MCRequest, 50)
	bridge := make(chan *gomem.MCResponse, 50)
	go execute(c, dispatch, bridge)
	go writeback(bridge, out)
	parse(in, dispatch)
}

func replyTo(c cuckoo.Cuckoo, in []byte, to *net.UDPAddr) {
	u, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer u.Close()

	var o bytes.Buffer
	setup(c, bytes.NewBuffer(in), &o)
	_, err = u.WriteTo(o.Bytes(), to)
	if err != nil {
		fmt.Println(err)
	}
}

func handleConnection(c cuckoo.Cuckoo, conn net.Conn) {
	setup(c, conn, conn)
	conn.Close()
}
