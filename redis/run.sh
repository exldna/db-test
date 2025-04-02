#!/bin/bash

resp_file=$1

time ( cat ${resp_file} | redis-cli --pipe )
