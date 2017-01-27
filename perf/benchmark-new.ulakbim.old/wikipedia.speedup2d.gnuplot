
set size 1.0, 0.6
set terminal postscript portrait enhanced "Helvetica" 14
set out "benchmark-new.ulakbim/wikipedia.speedup2d.ps"

set title "Speedup of wikipedia"
set xlabel "Rows x Columns"
set ylabel "Parallel Speedup"
set key autotitle columnheader
#set datafile missing '-'

set auto x

set yrange [0:]

set key left top
set key box lw 0.25

set style data histogram
set style histogram cluster gap 1
set style fill pattern 1 border -1
set boxwidth 0.5
set bmargin 5
    
set xtic rotate by -45
plot 'benchmark-new.ulakbim/wikipedia.speedup2d' using 2:xtic(1) 