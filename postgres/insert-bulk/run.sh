#!/bin/bash

DATA_FILE="$1"

psql -t -c "create database test"

psql test -t -c " \
create table if not exists user_transactions ( \
    user_addr character[26] not null, \
    tx_timestamp timestamp not null, \
    tx_hash character[32] not null \
);"

time psql test -c " \
COPY user_transactions(user_addr, tx_timestamp, tx_hash) \
FROM '${DATA_FILE}' \
WITH (FORMAT csv, HEADER true);"
