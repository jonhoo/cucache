package cuckoo_test

import (
	"bytes"
	"cuckood"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"testing"
	"time"
)

var never = time.Time{}

func TestSimple(t *testing.T) {
	c := cuckoo.New()
	c.Set("hello", []byte("world"), 0, never)
	v, ok := c.Get("hello")

	if !ok {
		t.Error("Get did not return successfully")
	}

	if string(v.Bytes) != "world" {
		t.Error("Get returned wrong string")
	}
}

func TestMany(t *testing.T) {
	c := cuckoo.New()

	for i := 0; i < 1e3; i++ {
		j := uint64(rand.Int63())
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, j)
		c.Set(strconv.FormatUint(j, 10), b, 0, never)
		v, ok := c.Get(strconv.FormatUint(j, 10))
		if !ok {
			t.Error("Concurrent get failed")
		}
		if !bytes.Equal(b, v.Bytes) {
			t.Error("Concurrent get did not return correct value")
		}
	}
}

type igtime struct {
	i      int
	insert time.Duration
	get    time.Duration
}

func TestConcurrent(t *testing.T) {
	runtime.GOMAXPROCS(4)
	c := cuckoo.New()

	ech := make(chan bool)
	errs := 0
	go func() {
		for range ech {
			errs++
		}
	}()

	os.Remove("results.log")
	res, _ := os.Create("results.log")
	tms := make(chan igtime)
	go func() {
		for tm := range tms {
			fmt.Fprintf(res, "%d %f %f\n", tm.i, tm.insert.Seconds(), tm.get.Seconds())
		}
		res.Close()
	}()

	var wg sync.WaitGroup
	ch := make(chan int)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := range ch {
				tm := igtime{}

				start := time.Now()

				j := i
				b := make([]byte, 8)
				binary.BigEndian.PutUint64(b, uint64(j))

				e := c.Set(strconv.Itoa(i), b, 0, never)
				tm.insert = time.Now().Sub(start)

				if e.T != cuckoo.STORED {
					ech <- true
					continue
				}

				start = time.Now()
				v, ok := c.Get(strconv.Itoa(i))
				tm.get = time.Now().Sub(start)

				if !ok {
					t.Error("Concurrent get failed")
				}
				if !bytes.Equal(b, v.Bytes) {
					t.Error("Concurrent get did not return correct value")
				}

				tm.i = i
				tms <- tm
			}
		}(i)
	}

	os.Remove("cpu.out")
	cpu, _ := os.Create("cpu.out")
	pprof.StartCPUProfile(cpu)
	for i := 0; i < 70e3; i++ {
		ch <- i

		if i%2e3 == 0 {
			fmt.Println(i)
		}
	}
	close(ch)
	wg.Wait()
	close(tms)

	fmt.Println("observed", errs, "insert errors")

	os.Remove("mem.out")
	mem, _ := os.Create("mem.out")
	pprof.WriteHeapProfile(mem)

	pprof.StopCPUProfile()
	cpu.Close()
}

func TestSameKey(t *testing.T) {
	runtime.GOMAXPROCS(4)
	c := cuckoo.New()

	get := func() {
		v, ok := c.Get("a")
		if !ok {
			t.Error("key lost")
		}
		if len(v.Bytes) != 1 || (v.Bytes[0] != 0x1 && v.Bytes[0] != 0x2) {
			t.Error("value is not one of the inserted values")
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b := []byte{0x1}
		for i := 0; i < 1e5; i++ {
			c.Set("a", b, 0, never)
			get()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		b := []byte{0x2}
		for i := 0; i < 1e5; i++ {
			c.Set("a", b, 0, never)
			get()
		}
	}()
	wg.Wait()
}
