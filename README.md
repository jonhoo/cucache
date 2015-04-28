# cucache
Fast PUT/GET/DELETE in-memory key-value store for lookaside caching.

[![Build Status](https://travis-ci.org/jonhoo/cucache.svg?branch=master)](https://travis-ci.org/jonhoo/cucache)

A mostly complete implementation the memcache
[text](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)
and
[binary](https://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol)
protocols can be found inside [cucache](cucache/). The binary protocol
has been tested using
[memcached-test](https://github.com/dustin/memcached-test), and the text
protocol with simple test cases extracted from the protocol
specification.

The implementation uses Cuckoo hashing along with several
[concurrency](https://www.cs.cmu.edu/~dga/papers/memc3-nsdi2013.pdf)
[optimizations](https://www.cs.princeton.edu/~mfreed/docs/cuckoo-eurosys14.pdf).
The implementation uses much of the code from [Dustin Sallings'
gomemcached](https://github.com/dustin/gomemcached) for the binary
protocol.

## Known limitations and outstanding things

  - Needs configurable debugging information output
  - The touch command is not implemented; see [dustin/gomemcached#12](https://github.com/dustin/gomemcached/pull/12)
  - Protocol should be tested against [mctest](https://github.com/victorkirkebo/mctest)

Current implementation notes can be found in [wip.md](wip.md).

## Want to use it?

Great! Please submit pull-requests and issues if you come across
anything you think is wrong. Note that this is very much a WIP, and I
give no guarantees about support.

## Why another memcached?

Cuckoo hashing is cool, and fast. Go is cool, and fast. Maybe the two
can outcompete the aging (though still very much relevant) memcached
while keeping the code nice and readable. Furthermore, as the Go runtime
improves over time, cucache might itself get faster *automatically*!

The hope is that fine-grained write locking and lock-free reading might
speed up concurrent access significantly, and allow better scalability
to many cores. While the traditional memcached wisdom of "just shard
your data more" works well most of the time, there comes a point where
you have some single key that is extremely hot, and then sharing simply
won't help you. You need to be able to distribute that keys load across
multiple cores. Although memcached does support multi-threaded
execution, the fact that it locks during reads is a potential scaling
bottleneck.

## Experimental results

cucache is currently slightly slower than memcached in terms of
over-the-network performance simply due to Go being slower than C for
many operations: network socket operations have more overhead, system
calls are slower, request and response marshalling is slower, and
goroutine scheduling and GC incur additional runtime cost. In terms of
pure performance (i.e. direct hash table operations), cuache is probably
significantly faster than memcached already.

See [benchmark/](benchmark/) for more in-depth performance evaluation.
