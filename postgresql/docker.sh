#!/bin/bash

BENCHMARK="$1"
DATA_FILE="$2"
RESULT_FILE="$3"

CONTAINER_NAME="bench-postgres"
BENCH_DIR="$(dirname $0)/${BENCHMARK}"

docker container run -d --name $CONTAINER_NAME -e POSTGRES_HOST_AUTH_METHOD=trust postgres

docker cp $DATA_FILE $CONTAINER_NAME:/tmp

docker cp "${BENCH_DIR}/benchmark.sh" $CONTAINER_NAME:/tmp
docker cp "${BENCH_DIR}/run.sh" $CONTAINER_NAME:/tmp

docker exec -i $CONTAINER_NAME /tmp/benchmark.sh /tmp/$(basename $DATA_FILE)

if [ -n "$RESULT_FILE" ]; then
    docker cp "$CONTAINER_NAME:$RESULT_FILE" .
fi

docker cp "$CONTAINER_NAME:log.txt" .

docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME
