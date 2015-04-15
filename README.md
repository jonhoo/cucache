# cuckoocache
Fast PUT/GET/DELETE in-memory key-value store for lookaside caching.

A mostly complete implementation the memcache
[text](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)
and
[binary](https://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol)
protocols can be found inside [cucache](cucache/). The binary protocol
has been tested using
[memcached-test](https://github.com/dustin/memcached-test).

The implementation uses Cuckoo hashing along with several [concurrency
optimizations](https://www.cs.princeton.edu/~mfreed/docs/cuckoo-eurosys14.pdf).
The implementation uses much of the code from [Dustin Sallings'
gomemcached](https://github.com/dustin/gomemcached) for the binary
protocol.

## Known limitations and outstanding things

  - Keyspace must be pre-allocated (!!!)
  - Unbounded buffering (quieted gets and #bytes for sets)
  - Multi-gets using text protocol do not work
  - Configurable debugging information output
  - The touch command for the text protocol does not work (no binary equivalent)
  - Test protocol against [mctest](https://github.com/victorkirkebo/mctest)

## Want to use it?

Great! Please submit pull-requests and issues if you come across
anything you think is wrong. Note that this is very much a WIP, and I
give no guarantees about support.

## Why another memcached?

Cuckoo hashing is cool, and fast. Go is cool, and fast. Maybe the two
can outcompete the aging memcached. In particular, the fine-grained
write locking and lock-free reading might speed up concurrent access
significantly.

## Experimental results

Full results can be found in [cucache/benchmark](cucache/benchmark). The
results below have been truncated.

```
$ memcached &
$ memtier_benchmark -p 11211 -P memcache_binary -n 50000
Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
------------------------------------------------------------------------
Sets       13081.92          ---          ---      1.40800      1007.81
Gets      130801.96        27.68    130774.28      1.40300      4967.66
Totals    143883.89        27.68    130774.28      1.40300      5975.47

Request Latency Distribution
Type        <= msec      Percent
------------------------------------------------------------------------
SET               0        24.17
SET               1        82.66
SET               2        97.05
SET               3        99.04
SET               4        99.63
SET               5        99.79
SET               6        99.86
SET               7        99.90
SET               8        99.92
SET               9        99.94
SET              10        99.96
SET              11        99.98
SET              12        99.98
SET              13        99.99
---
GET               0        24.31
GET               1        82.82
GET               2        97.06
GET               3        99.06
GET               4        99.64
GET               5        99.80
GET               6        99.87
GET               7        99.91
GET               8        99.93
GET               9        99.95
GET              10        99.96
GET              11        99.98
GET              12        99.99
```
```
$ GOMAXPROCS=2 dev/go/bin/cucache &
$ memtier_benchmark -p 11211 -P memcache_binary -n 50000
Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
------------------------------------------------------------------------
Sets        8144.99          ---          ---      2.25100       627.48
Gets       81439.15        16.13     81423.03      2.24700      3092.94
Totals     89584.14        16.13     81423.03      2.24700      3720.41

Request Latency Distribution
Type        <= msec      Percent
------------------------------------------------------------------------
SET               0         2.38
SET               1        42.06
SET               2        86.07
SET               3        98.62
SET               4        99.07
SET               5        99.17
SET               6        99.25
SET               7        99.33
SET               8        99.41
SET               9        99.51
SET              10        99.67
SET              11        99.86
SET              12        99.95
SET              13        99.97
SET              14        99.99
---
GET               0         2.39
GET               1        42.22
GET               2        86.18
GET               3        98.64
GET               4        99.08
GET               5        99.17
GET               6        99.25
GET               7        99.34
GET               8        99.42
GET               9        99.51
GET              10        99.67
GET              11        99.87
GET              12        99.95
GET              13        99.98
GET              14        99.99
```

### CPU Profile:

![CPU profile of cucache](cucache/bechmark/profile.svg)
