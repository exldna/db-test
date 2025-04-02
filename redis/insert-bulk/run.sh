#!/bin/bash

resp_file=$1

until pg_isready -h localhost -p 5432 -U postgres; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

time ( cat ${resp_file} | redis-cli --pipe )
