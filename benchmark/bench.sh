#!/bin/bash
command -v numactl >/dev/null 2>&1
no_numa=$?

if [ $no_numa -eq 1 ]; then
	echo "no numactl, so cannot force core locality; exiting..." > /dev/stderr
	exit 1
fi

scores="$1"; shift
ccores="$1"; shift
((needed=scores+ccores));

# where are we?
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

cores=$(numactl -H | grep cpus | sed 's/.*: //' | paste -sd' ')
cores=($cores)
ncores=${#cores[@]}

if ((needed>ncores)); then
	echo "Cannot use more server+client cores ($needed) than there are CPU cores ($ncores)" >/dev/stderr
	exit 1
fi

srange=$(echo ${cores[@]} | cut -d' ' -f1-${scores} | tr ' ' ',')
crange=$(echo ${cores[@]} | rev | cut -d' ' -f1-${ccores} | rev | tr ' ' ',')

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

# run the server
echo numactl -C $srange env GOMAXPROCS=$scores "${args[@]}" -p 2222 -U 2222 > /dev/stderr
numactl -C $srange env GOMAXPROCS=$scores "${args[@]}" -p 2222 -U 2222 &
pid=$!

# let it initialize
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

# run the client
echo numactl -C $crange memtier_benchmark -p 2222 -P memcache_binary $memargs > /dev/stderr
numactl -C $crange memtier_benchmark -p 2222 -P memcache_binary $memargs 2>/dev/null

# terminate the server
kill $pid 2>/dev/null
wait $pid 2>/dev/null
