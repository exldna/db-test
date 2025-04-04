#!/bin/bash

DATA_FILE="$1"

cat ${DATA_FILE} | redis-cli --pipe

$(dirname $0)/run.sh > timings.log

awk '{sum+=$4; count++} END {print "Avg:",sum/count,"ms"}' timings.log

awk '{sum[$1]+=$4; count[$1]++} END {for(k in sum) print k,sum[k]/count[k]"ms"}' timings.log

sort -k4 -nr results.txt | head -5
