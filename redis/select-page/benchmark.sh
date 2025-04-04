#!/bin/bash

DATA_FILE="$1"

cat ${DATA_FILE} | redis-cli --pipe

$(dirname $0)/run.sh > timings.log

awk '{sum+=$4; count++} END {print "Avg:",sum/count,"ms"}' timings.log

sort -k4 -nr timings.log | head -5
