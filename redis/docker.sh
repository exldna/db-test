#!/bin/bash

CONTAINER_NAME="bench-redis"
DATA_FILE=$1

docker container run -d --name $CONTAINER_NAME redis
docker cp $(dirname $0)/run.sh $CONTAINER_NAME:/tmp
cat $DATA_FILE | docker exec $CONTAINER_NAME tar -x -C /tmp

docker exec /tmp/run.sh /tmp/items

docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME
