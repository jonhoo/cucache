#!/usr/bin/gnuplot -p
set key top left
set xlabel "CPU Cores"
set ylabel "ops/s"
plot \
  "< grep memcached cucache.dat | grep -- '-' | sed 's/-memcached-/ /' | grep Gets" u 2:4 dt 2 lt 1 t "Memcached get hit" w linespoints,\
  "< grep cucache cucache.dat   | grep -- '-' | sed 's/-cucache-/ /'   | grep Gets"   u 2:4 lt 1 t "Cucache get hit" w linespoints,\
  "< grep memcached cucache.dat | grep -- '-' | sed 's/-memcached-/ /' | grep Sets" u 2:4 dt 2 lt 2 t "Memcached set" w linespoints,\
  "< grep cucache cucache.dat   | grep -- '-' | sed 's/-cucache-/ /'   | grep Sets"   u 2:4 lt 2 t "Cucache set" w linespoints,\
  "< grep memcached cucache.dat | grep -- '-' | sed 's/-memcached-/ /' | grep Gets"    u 2:5 dt 2 lt 3 t "Memcached get miss" w linespoints,\
  "< grep cucache cucache.dat   | grep -- '-' | sed 's/-cucache-/ /'   | grep Gets"   u 2:5 lt 3 t "Cucache get miss" w linespoints,\
  "< grep memcached cucache.dat | grep -- '-' | sed 's/-memcached-/ /' | grep Gets" u 2:6 dt 2 lt 4 t "Memcached get total" w linespoints,\
  "< grep cucache cucache.dat   | grep -- '-' | sed 's/-cucache-/ /'   | grep Gets"   u 2:6 lt 4 t "Cucache get total" w linespoints,\
  #
