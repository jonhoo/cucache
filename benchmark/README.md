cucache has has been benchmarked on two multi-core machines thus far,
[ben](ben/) and [tom](tom/). Results can be found by going to each of
those directory. The former is a 40-core machine with four NUMA nodes,
and the latter is a slower 48-core machine with eight NUMA nodes.

Benchmarks were performed with
[memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) on
the same machine as the servers with all connections going over
loopback. The exact parameters used can be seen in [bench.sh](bench.sh).
Experimental results suggest that when a real network link is used,
memcached and cucache perform roughly the same. When used across
loopback, cucache scales better than memcached, though its absolute
performance is lower when the number of cores is small.

It is worth noting that this benchmark is still somewhat artificial. It
does not model key contention, which is likely to be an issue for
memcached, but not so much for cucache. Furthermore, as the benchmark
has to be run on a single machine to not have the network interface be
the bottleneck, clients will eventually struggle to generate enough load
to saturate the server's capacity. We can see this happening on ben.
