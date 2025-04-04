#!/bin/bash

DATA_FILE="$1"

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
