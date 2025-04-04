#!/bin/bash

DATA_FILE="$1"

until pg_isready -h localhost -p 5432; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

psql -U postgres -t -c "create database test"

psql -U postgres -d test -t -c " \
    create table if not exists user_transactions ( \
        user_address text not null, \
        transaction_timestamp int not null, \
        transaction_hash text not null \
    );"

psql -U postgres -d test -c " \
    COPY user_transactions( \
        user_address, \
        transaction_timestamp, \
        transaction_hash \
    ) \
    FROM '${DATA_FILE}' \
    WITH (FORMAT csv);"

$(dirname $0)/run.sh |& tee log.txt
