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
mi=9
seen_mc=0
for f in "${versions[@]}"; do
	# Tidy up title
	t="$f"
	t=$(echo "$t" | perl -pe 's/-(tom|ben)(-.*|$)/\2 on \1/')
	t=$(echo "$t" | sed 's/^exp-//')
	t=$(echo "$t" | sed 's/^-memcached-//')
	t=$(echo "$t" | perl -pe 's/^(.*)-memcached-/memcached /')

	# Is this a memcached entry?
	mc=0
	echo "$f" | grep memcached >/dev/null
	[ $? -eq 0 ] && mc=1

	# line type
	lt=$i
	[ $mc -eq 1 ] && lt=$mi

	# Add plot lines; tt is needed to not print ' get hit' for hidden
	# memcached lines
	tt=""
	tt="get hit ($t)"
	#out+=" \"< grep '$f' cucache.dat | perl -pe 's/^.*?-(\\\\d+)/\\\\1/' | grep Gets\" u 1:3 dt 1 pt 1 lt $lt t '$tt' w linespoints,"
	tt="set ($t)"
	out+=" \"< grep '$f' cucache.dat | perl -pe 's/^.*?-(\\\\d+)/\\\\1/' | grep Sets\" u 1:3 dt 2 pt 2 lt $lt t '$tt' w linespoints,"
	tt="get miss ($t)"
	#out+=" \"< grep '$f' cucache.dat | perl -pe 's/^.*?-(\\\\d+)/\\\\1/' | grep Gets\" u 1:4 dt 3 pt 3 lt $lt t '$tt' w linespoints,"
	tt="get ($t)"
	out+=" \"< grep '$f' cucache.dat | perl -pe 's/^.*?-(\\\\d+)/\\\\1/' | grep Gets\" u 1:5 dt 4 pt 4 lt $lt t '$tt' w linespoints,"

	# Only "real" lines increment dash type
	[ $mc -eq 0 ] && ((i=i+1))
	[ $mc -eq 1 ] && ((mi=mi-1))
done

# Strip trailing comma
out=${out%,}

echo "$out"
echo "$out" | gnuplot -p
