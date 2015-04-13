package cuckoo_test

import (
	"cuckood"
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

func TestSimple(t *testing.T) {
	c := cuckoo.New()
	c.Insert("hello", "world")
	v, ok := c.Get("hello")

	if !ok {
		t.Error("Get did not return successfully")
	}

	switch v := v.(type) {
	case string:
		if v != "world" {
			t.Error("Get returned wrong string")
		}
	default:
		t.Error("Get did not return a string")
	}
}

func TestMany(t *testing.T) {
	c := cuckoo.New()

	for i := 0; i < 1e5; i++ {
		j := rand.Int()
		c.Insert(strconv.Itoa(j), j)
		v, ok := c.Get(strconv.Itoa(j))
		if !ok {
			t.Error("Concurrent get failed")
		}
		if vj, ok := v.(int); !ok || j != vj {
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
				e := c.Insert(strconv.Itoa(i), i)
				tm.insert = time.Now().Sub(start)

				if e != nil {
					ech <- true
					continue
				}

				start = time.Now()
				v, ok := c.Get(strconv.Itoa(i))
				tm.get = time.Now().Sub(start)

				if !ok {
					t.Error("Concurrent get failed")
				}
				if vi, ok := v.(int); !ok || i != vi {
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
	for i := 0; i < 5e6; i++ {
		ch <- rand.Int()

		if i%2e5 == 0 {
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
