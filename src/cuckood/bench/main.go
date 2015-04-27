package main

import (
	"cuckood"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

func main() {
	cpuprofile := flag.Bool("cpuprofile", false, "CPU profile")
	n := flag.Int("n", 10000, "Number of requests to make")
	s := flag.Uint64("s", 0, "Initial db size")
	flag.Parse()

	c := cuckoo.New(*s)

	var pf *os.File
	var err error
	if *cpuprofile {
		fmt.Fprintln(os.Stderr, "starting CPU profiling of set/get")
		pf, err = os.Create("set.out")
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create CPU profile file set.out: %v\n", err)
			return
		}
		err = pprof.StartCPUProfile(pf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not start CPU profiling: %v\n", err)
			return
		}
	}

	at := *n / 10
	v := []byte{0x01}
	var mem runtime.MemStats
	rand.Seed(1)
	for i := 0; i < *n; i++ {
		k := []byte(fmt.Sprintf("%d-%d", i, rand.Int63()))

		sstart := time.Now()
		c.Set(k, v, 0, time.Time{})

		gstart := time.Now()
		c.Get(k)

		end := time.Now()

		if i%at == 0 {
			runtime.ReadMemStats(&mem)
			fmt.Println(i, gstart.Sub(sstart).Seconds(), end.Sub(gstart).Seconds(), mem.Alloc, mem.Mallocs)
			fmt.Fprintln(os.Stderr, i)
		} else {
			fmt.Println(i, gstart.Sub(sstart).Seconds(), end.Sub(gstart).Seconds())
		}
	}

	if pf != nil {
		pprof.StopCPUProfile()
		err := pf.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, "could not end cpu profile:", err)
		}

		fmt.Fprintln(os.Stderr, "starting CPU profiling of get")
		pf, err = os.Create("get.out")
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create CPU profile file get.out: %v\n", err)
			return
		}
		err = pprof.StartCPUProfile(pf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not start CPU profiling: %v\n", err)
			return
		}
	}

	var num int
	var avg float64
	rand.Seed(1)
	for i := 0; i < *n; i++ {
		k := []byte(fmt.Sprintf("%d-%d", i, rand.Int63()))

		start := time.Now()
		c.Get(k)
		end := time.Now()

		avg = (end.Sub(start).Seconds() + avg*float64(num)) / float64(num+1)
		num++
	}

	fmt.Fprintf(os.Stderr, "average get speed: %.2fus\n", avg*1000000)

	if pf != nil {
		pprof.StopCPUProfile()
		err := pf.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, "could not end cpu profile:", err)
		}
	}

}
