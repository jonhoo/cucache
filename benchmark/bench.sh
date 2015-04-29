#!/bin/bash
ncores=$(lscpu | grep "^CPU(s):" | sed 's/.* //')
scores="$1"; shift
ccores="$1"; shift
((startc=ncores-ccores))

if ((scores+ccores>ncores)); then
	echo "Cannot use more server+client cores than there are CPU cores" >/dev/stderr
	exit 1
fi

((ends=scores-1))
((endc=ncores-1))

command -v numactl >/dev/null 2>&1
no_numa=$?

args=()
for i in "$@"; do
	if [ "$i" == "CCORES" ]; then
		args+=("$ccores")
	elif [ "$i" == "SCORES" ]; then
		args+=("$scores")
	else
		args+=("$i")
	fi
done

if [ $no_numa -eq 1 ]; then
	echo env GOMAXPROCS=$scores "${args[@]}" -p 2222 -U 2222
	env GOMAXPROCS=$scores "${args[@]}" -p 2222 -U 2222 &
	pid=$!
else
	echo numactl -C 0-$ends env GOMAXPROCS=$scores "${args[@]}" -p 2222 -U 2222
	numactl -C 0-$ends env GOMAXPROCS=$scores "${args[@]}" -p 2222 -U 2222 &
	pid=$!
fi

sleep 1

memargs=""
memargs="$memargs -n 20000" # lots o' requests

# this number is taken out of thin air
# if you have an good estimate, please let me know
concurrent_clients=200
((nc=concurrent_clients/ccores))
memargs="$memargs -t $ccores -c $nc"

# numbers below from
# http://www.ece.eng.wayne.edu/~sjiang/pubs/papers/atikoglu12-memcached.pdf

# each request has keys of ~32b, and values of ~200b
memargs="$memargs --key-prefix trytomakekey32byteslong"
memargs="$memargs --data-size-range=150-350"

# set:get varies between 1:30 to 1:500
memargs="$memargs --ratio 1:100"

# number of keys is hard to extract from the paper, but given ~100000 req/s,
# and ~30% unique keys, number of keys can be 0.3*100000
memargs="$memargs --key-minimum=1"
memargs="$memargs --key-maximum=30000"

# let's say keys are roughly normal distributed,
# but a small number of keys (1%) are hot.
memargs="$memargs --key-pattern=G:G"
memargs="$memargs --key-stddev=300"

if [ $no_numa -eq 1 ]; then
	echo memtier_benchmark -p 2222 -P memcache_binary $memargs
	memtier_benchmark -p 2222 -P memcache_binary $memargs 2>/dev/null
else
	echo numactl -C $startc-$endc memtier_benchmark -p 2222 -P memcache_binary $memargs
	numactl -C $startc-$endc memtier_benchmark -p 2222 -P memcache_binary $memargs 2>/dev/null
fi

kill $pid
wait $pid 2>/dev/null
