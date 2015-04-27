set term pngcairo size 1200,600
set output "bench.png"
set logscale y
#set logscale y2
#set y2tics
set ylabel "us/op"
#set y2label "MB allocated"
set rmargin 5

set arrow from (2**12)*8,1e-1 to (2**12)*8,1e7 nohead lc rgb 'red'
set arrow from (2**13)*8,1e-1 to (2**13)*8,1e7 nohead lc rgb 'red'
set arrow from (2**14)*8,1e-1 to (2**14)*8,1e7 nohead lc rgb 'red'
set arrow from (2**15)*8,1e-1 to (2**15)*8,1e7 nohead lc rgb 'red'
set arrow from (2**16)*8,1e-1 to (2**16)*8,1e7 nohead lc rgb 'red'
set arrow from (2**17)*8,1e-1 to (2**17)*8,1e7 nohead lc rgb 'red'

plot \
	'bench.dat' u 1:($2*1000000) t 'set', \
	'' u 1:($3*1000000) t 'get'
	#'' u 1:($4/1000000.0) t "alloc'd" axis x1y2
