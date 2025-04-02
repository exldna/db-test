#!/bin/bash

DATA_FILE="$1"

until pg_isready -h localhost -p 5432; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

sudo -u postgres psql -t -c "create database test"

sudo -u postgres psql test -t -c " \
create table if not exists user_transactions ( \
    user_addr character[26] not null, \
    tx_timestamp timestamp not null, \
    tx_hash character[32] not null \
);"

sudo -u postgres time psql test -c " \
COPY user_transactions(user_addr, tx_timestamp, tx_hash) \
FROM '${DATA_FILE}' \
WITH (FORMAT csv, HEADER true);"
