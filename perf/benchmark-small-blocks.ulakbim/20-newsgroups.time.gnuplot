
set size 1.0, 0.6
set terminal postscript portrait enhanced "Helvetica" 14
set out "benchmark-small-blocks.ulakbim/20-newsgroups.time.ps"

set title "Running time of 20-newsgroups"
set xlabel "Processors"
set ylabel "Parallel Running Time"
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
    
set key above

plot 'benchmark-small-blocks.ulakbim/20-newsgroups.time' using 2:xtic(1) , '' u 3:xtic(1) , '' u 4:xtic(1) , '' u 5:xtic(1) , '' u 6:xtic(1) , '' u 7:xtic(1) 