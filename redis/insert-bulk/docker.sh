#!/bin/bash

DATA_FILE="$1"
CONTAINER_NAME="bench-redis"

docker container run -d --name $CONTAINER_NAME redis
docker cp $(dirname $0)/run.sh $CONTAINER_NAME:/tmp
cat $DATA_FILE | docker exec -i $CONTAINER_NAME tar -x -C /tmp

docker exec -i $CONTAINER_NAME /tmp/run.sh /tmp/items

docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME
