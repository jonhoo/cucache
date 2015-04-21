#!/usr/bin/gnuplot -p
set xlabel "CPU Cores"
set ylabel "misses|sets/s"
set y2label "hits/s"
set y2tics
plot \
  "< grep memcached cucache.dat | sed s/memcached-// | grep Gets" u 1:3 axis x1y2 dt 2 lt 1 t "Memcached get hit" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Gets"     u 1:3 axis x1y2 lt 1 t "Cucache get hit" w linespoints,\
  "< grep memcached cucache.dat | sed s/memcached-// | grep Sets" u 1:3 dt 2 lt 2 t "Memcached set" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Sets"     u 1:3 lt 2 t "Cucache set" w linespoints,\
  "< grep memcached cucache.dat | sed s/memcached-// | grep Gets" u 1:4 dt 2 lt 3 t "Memcached get miss" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Gets"     u 1:4 lt 3 t "Cucache get miss" w linespoints,\
  #
