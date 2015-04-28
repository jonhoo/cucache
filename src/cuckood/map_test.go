package cuckoo_test

import (
	"bytes"
	"cuckood"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

var never = time.Time{}

func TestSimple(t *testing.T) {
	c := cuckoo.New(0)
	c.Set([]byte("hello"), []byte("world"), 0, never)
	v, ok := c.Get([]byte("hello"))

	if !ok {
		t.Error("Get did not return successfully")
	}

	if string(v.Bytes) != "world" {
		t.Error("Get returned wrong string")
	}
}

func TestMany(t *testing.T) {
	c := cuckoo.New(1 << 10)

	for i := 0; i < 1<<9; i++ {
		j := uint64(rand.Int63())
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, j)
		c.Set([]byte(strconv.FormatUint(j, 10)), b, 0, never)
		v, ok := c.Get([]byte(strconv.FormatUint(j, 10)))
		if !ok {
			t.Error("Concurrent get failed for key", []byte(strconv.FormatUint(j, 10)))
			return
		}
		if !bytes.Equal(b, v.Bytes) {
			t.Error("Concurrent get did not return correct value")
		}
	}
}

func TestResize(t *testing.T) {
	c := cuckoo.New(1 << 9)

	for i := 0; i < 1<<10; i++ {
		j := uint64(rand.Int63())
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, j)
		c.Set([]byte(strconv.FormatUint(j, 10)), b, 0, never)
		v, ok := c.Get([]byte(strconv.FormatUint(j, 10)))
		if !ok {
			t.Error("Concurrent get failed for key", []byte(strconv.FormatUint(j, 10)))
			return
		}
		if !bytes.Equal(b, v.Bytes) {
			t.Error("Concurrent get did not return correct value")
		}
	}
}

func TestConcurrent(t *testing.T) {
	runtime.GOMAXPROCS(4)
	c := cuckoo.New(1 << 16)

	ech := make(chan bool)
	errs := 0
	go func() {
		for range ech {
			errs++
		}
	}()

	var wg sync.WaitGroup
	ch := make(chan int)
	for i := 0; i < 1e3; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := range ch {
				j := i
				b := make([]byte, 8)
				binary.BigEndian.PutUint64(b, uint64(j))

				e := c.Set([]byte(strconv.Itoa(i)), b, 0, never)

				if e.T != cuckoo.STORED {
					ech <- true
					continue
				}

				v, ok := c.Get([]byte(strconv.Itoa(i)))

				if !ok {
					t.Error("Concurrent get failed")
				}
				if !bytes.Equal(b, v.Bytes) {
					t.Error("Concurrent get did not return correct value")
				}
			}
		}(i)
	}

	for i := 0; i < 1<<15; i++ {
		ch <- i

		if i%(1<<12) == 0 {
			fmt.Println(i)
		}
	}
	close(ch)
	wg.Wait()

	if errs != 0 {
		t.Error("observed", errs, "insert errors")
	}
}

func TestSameKey(t *testing.T) {
	runtime.GOMAXPROCS(4)
	c := cuckoo.New(1 << 10)

	get := func() {
		v, ok := c.Get([]byte("a"))
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
			c.Set([]byte("a"), b, 0, never)
			get()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		b := []byte{0x2}
		for i := 0; i < 1e5; i++ {
			c.Set([]byte("a"), b, 0, never)
			get()
		}
	}()
	wg.Wait()
}

func TestNoEvict(t *testing.T) {
	c := cuckoo.New(uint64(cuckoo.ASSOCIATIVITY))

	for i := 0; i < cuckoo.ASSOCIATIVITY; i++ {
		res := c.Add(append([]byte("hello"), byte(i)), []byte("world"), 0, never)
		if res.T != cuckoo.STORED {
			t.Error("could not insert element", res)
			return
		}
	}

	// table should now be full

	res := c.Add(append([]byte("hello"), byte(cuckoo.ASSOCIATIVITY+1)), []byte("world"), 0, never)
	if res.T != cuckoo.STORED {
		t.Error("table did not make room for new item")
		return
	}

	out := 0
	for i := 0; i < cuckoo.ASSOCIATIVITY; i++ {
		_, ok := c.Get(append([]byte("hello"), byte(i)))
		if !ok {
			out++
		}
	}

	if out != 0 {
		t.Error(out, "items were evicted, when eviction is disabled")
	}
}

func TestEvict(t *testing.T) {
	c := cuckoo.New(uint64(cuckoo.ASSOCIATIVITY))

	for i := 0; i < cuckoo.ASSOCIATIVITY; i++ {
		res := c.Add(append([]byte("hello"), byte(i)), []byte("world"), 0, never)
		if res.T != cuckoo.STORED {
			t.Error("could not insert element", res)
			return
		}
	}

	// table should now be full
	c.EnableEviction()

	res := c.Add(append([]byte("hello"), byte(cuckoo.ASSOCIATIVITY+1)), []byte("world"), 0, never)
	if res.T != cuckoo.STORED {
		t.Error("table did not evict to make room for new item")
		return
	}

	if c.Capacity() != uint64(cuckoo.ASSOCIATIVITY) {
		t.Error("table was resized when eviction was possible")
		return
	}

	out := 0
	for i := 0; i < cuckoo.ASSOCIATIVITY; i++ {
		_, ok := c.Get(append([]byte("hello"), byte(i)))
		if !ok {
			out++
		}
	}

	if out != 1 {
		t.Error(out, "items were evicted, when only one should have been")
	}
}
