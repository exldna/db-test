#!/bin/bash

BENCHMARK="$1"
DATA_FILE="$2"

CONTAINER_NAME="bench-redis"
BENCH_DIR="$(dirname $0)/${BENCHMARK}"

docker container run -d --name $CONTAINER_NAME redis

cat $DATA_FILE | docker exec -i $CONTAINER_NAME tar -x -C /tmp

docker cp $BENCH_DIR/benchmark.sh $CONTAINER_NAME:/tmp
docker cp $BENCH_DIR/run.sh $CONTAINER_NAME:/tmp

docker exec -i $CONTAINER_NAME /tmp/benchmark.sh /tmp/items

docker rm $CONTAINER_NAME
