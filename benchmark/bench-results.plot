#!/usr/bin/gnuplot -p
set key top left
set xlabel "CPU Cores"
set ylabel "ops/s"
plot \
  "< grep memcached cucache.dat | sed s/memcached-// | grep Gets" u 1:3 dt 2 lt 1 t "Memcached get hit" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Gets"     u 1:3 lt 1 t "Cucache get hit" w linespoints,\
  "< grep memcached cucache.dat | sed s/memcached-// | grep Sets" u 1:3 dt 2 lt 2 t "Memcached set" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Sets"     u 1:3 lt 2 t "Cucache set" w linespoints,\
  "< grep memcached cucache.dat | sed s/memcached-// | grep Gets" u 1:4 dt 2 lt 3 t "Memcached get miss" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Gets"     u 1:4 lt 3 t "Cucache get miss" w linespoints,\
  "< grep memcached cucache.dat | sed s/memcached-// | grep Gets" u 1:5 dt 2 lt 4 t "Memcached get total" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Gets"     u 1:5 lt 4 t "Cucache get total" w linespoints,\
  #
