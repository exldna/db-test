#!/bin/bash

DATA_FILE="$1"
CONTAINER_NAME="bench-postgres"

docker container run -it --name $CONTAINER_NAME -e POSTGRES_HOST_AUTH_METHOD=trust postgres
docker cp $(dirname $0)/run.sh $CONTAINER_NAME:/tmp
docker cp $DATA_FILE $CONTAINER_NAME:/tmp

# wait for postgres to be running
while ! nc -z 127.0.0.1 5432; do sleep 1; done;

docker exec -i $CONTAINER_NAME /tmp/run.sh /tmp/$(basename $DATA_FILE)

docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME
