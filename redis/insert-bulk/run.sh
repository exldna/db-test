#!/bin/bash

DATA_FILE="$1"

time ( cat $DATA_FILE | redis-cli --pipe )
