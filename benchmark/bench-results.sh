#!/bin/sh
# rev because otherwise exp-pre will be sorted before tom, but after ben
versions=$(awk '{print $1}' cucache.dat | sed 's/-[0-9]*$//' | sort -u | rev | sort | rev)
versions=($versions)

read -r -d '' out <<'EOF'
set key top left
set xlabel "CPU Cores"
set ylabel "ops/s"
plot
EOF

i=2
seen_mc=0
for f in "${versions[@]}"; do
	dt=$i

	# Tidy up title
	t="$f"
	t=$(echo "$t" | perl -pe 's/-(tom|ben)(-.*|$)/\2 on \1/')
	t=$(echo "$t" | sed 's/^exp-//')

	# Is this a memcached entry?
	echo "$f" | grep memcached >/dev/null
	[ $? -eq 0 ] && dt=1 && t="Memcached"

	# No title for later memcached entries
	[ $dt -eq 1 ] && [ $seen_mc -eq 1 ] && t=""
	[ $dt -eq 1 ] && seen_mc=1

	# Add plot lines; tt is needed to not print ' get hit' for hidden
	# memcached lines
	tt=""
	[ -n "$t" ] && tt="get hit ($t)"
	#out+=" \"< grep '$f' cucache.dat | perl -pe 's/^.*?-(\\\\d+)/\\\\1/' | grep Gets\" u 1:3 dt $dt lt 1 t '$tt' w linespoints,"
	[ -n "$t" ] && tt="set ($t)"
	out+=" \"< grep '$f' cucache.dat | perl -pe 's/^.*?-(\\\\d+)/\\\\1/' | grep Sets\" u 1:3 dt $dt lt 2 t '$tt' w linespoints,"
	[ -n "$t" ] && tt="get miss ($t)"
	#out+=" \"< grep '$f' cucache.dat | perl -pe 's/^.*?-(\\\\d+)/\\\\1/' | grep Gets\" u 1:4 dt $dt lt 3 t '$tt' w linespoints,"
	[ -n "$t" ] && tt="get ($t)"
	out+=" \"< grep '$f' cucache.dat | perl -pe 's/^.*?-(\\\\d+)/\\\\1/' | grep Gets\" u 1:5 dt $dt lt 4 t '$tt' w linespoints,"

	# Only "real" lines increment dash type
	[ $dt -gt 1 ] && ((i=i*2))
done

# Strip trailing comma
out=${out%,}

echo "$out"
echo "$out" | gnuplot -p
