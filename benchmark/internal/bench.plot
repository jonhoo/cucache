set term pngcairo size 1200,600
set output "bench.png"
set logscale y
#set logscale y2
#set y2tics
set ylabel "us/op"
#set y2label "MB allocated"
set rmargin 5

set arrow from 80000,1 to 80000,1e6 nohead lc rgb 'red'
set arrow from 160000,1 to 160000,1e6 nohead lc rgb 'red'
set arrow from 320000,1 to 320000,1e6 nohead lc rgb 'red'
set arrow from 640000,1 to 640000,1e6 nohead lc rgb 'red'
set arrow from 1280000,1 to 1280000,1e6 nohead lc rgb 'red'
set arrow from 2560000,1 to 2560000,1e6 nohead lc rgb 'red'

plot \
	'bench.dat' u 1:($2*1000000) t 'set', \
	'' u 1:($3*1000000) t 'get'
	#'' u 1:($4/1000000.0) t "alloc'd" axis x1y2
