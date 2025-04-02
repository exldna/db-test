#!/bin/bash

DATA_FILE="$1"

until pg_isready -h localhost -p 5432; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

psql -U postgres -t -c "create database test"

psql -U postgres -d test -t -c " \
    create table if not exists user_transactions ( \
        user_addr text not null, \
        tx_timestamp int not null, \
        tx_hash text not null \
    );"

time (
    psql -U postgres -d test -c " \
        COPY user_transactions(user_addr, tx_timestamp, tx_hash) \
        FROM '${DATA_FILE}' \
        WITH (FORMAT csv);"
)
