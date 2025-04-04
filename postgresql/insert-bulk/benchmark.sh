#!/bin/bash

DATA_FILE="$1"

until pg_isready -h localhost -p 5432; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

./run.sh $DATA_FILE |& tee log.txt

cat log.txt |
    grep -P '^real|^Error' |
    sed -r -e 's/^Error.*$/null/; s/^real\s*([0-9.]+)m([0-9.]+)s$/\1 \2/' |
    awk '{ if ($2) { print $1 * 60 + $2 } else { print $1 } }'
