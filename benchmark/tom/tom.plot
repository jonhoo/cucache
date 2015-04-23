#!/usr/bin/gnuplot -p
set key top left
set term pngcairo size 1200,600
set output "tom.png"
set xlabel "CPU Cores"
set ylabel "median ops/s"
plot \
  "< grep memcached cucache.dat | sed s/memcached-// | grep Sets" u 1:3 dt 2 lt 1 t "Memcached set" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Sets"     u 1:3 lt 1 t "Cucache set" w linespoints,\
  "< grep memcached cucache.dat | sed s/memcached-// | grep Gets" u 1:5 dt 2 lt 2 t "Memcached get" w linespoints,\
  "< grep cucache cucache.dat | sed s/cucache-// | grep Gets"     u 1:5 lt 2 t "Cucache get" w linespoints,\
  #
