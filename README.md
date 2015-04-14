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

  - Timed flush does not work
  - Keyspace must be pre-allocated (!!!)
  - Delay response to quiet GETs
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
significantly. Experimental results pending.
