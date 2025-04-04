#!/bin/bash

# Configuration
DB_URI="postgresql://postgres@localhost:5432/test"
OUTPUT_FILE="results.csv"
TOP_USERS_LIMIT=1000
BATCH_SIZE=100

# Initialize
init_benchmark() {
    echo "user_address,transaction_count,batch_number,batch_size,batch_read_time_ms" > "$OUTPUT_FILE"
}

# Database functions
execute_query() {
    psql "$DB_URI" -c "$1" -t -A -F'|'
}

# Data processing
get_top_users() {
    execute_query "
        SELECT user_address, count(*) 
        FROM user_transactions 
        GROUP BY user_address 
        ORDER BY count(*) DESC 
        LIMIT $TOP_USERS_LIMIT"
}

calculate_total_batches() {
    local total=0
    while IFS='|' read -r _ count; do
        total=$((total + (count + BATCH_SIZE - 1) / BATCH_SIZE))
    done <<< "$1"
    echo "$total"
}

process_batch() {
    local addr=$1 count=$2 batch_num=$3
    local offset=$(( (batch_num - 1) * BATCH_SIZE ))
    local limit=$(( batch_num * BATCH_SIZE <= count ? BATCH_SIZE : count % BATCH_SIZE ))

    local start=$(date +%s%3N)
    execute_query "
        SELECT transaction_timestamp 
        FROM (
            SELECT transaction_timestamp, 
                ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) as rn
            FROM user_transactions 
            WHERE user_address = '$addr'
        ) sub 
        WHERE rn > $offset AND rn <= $((offset + limit))" >/dev/null
    local duration=$(( $(date +%s%3N) - start ))

    echo "$addr,$count,$batch_num,$limit,$duration" >> "$OUTPUT_FILE"
}

process_user() {
    local addr=$1 count=$2 total_batches=$3 processed_ref=$4
    local batches=$(( (count + BATCH_SIZE - 1) / BATCH_SIZE ))

    for ((i=1; i<=batches; i++)); do
        process_batch "$addr" "$count" "$i"
        eval "$processed_ref=$((processed + 1))"
        printf "\rProgress: %d/%d batches" "${!processed_ref}" "$total_batches"
    done
}

main() {
    init_benchmark
    
    local users=$(get_top_users)
    local total_batches=$(calculate_total_batches "$users")
    
    [ "$total_batches" -eq 0 ] && { echo "No data to process"; exit; }

    local processed=0
    while IFS='|' read -r addr count; do
        addr=${addr// /}
        count=${count// /}
        [ -z "$addr" ] && continue

        process_user "$addr" "$count" "$total_batches" "processed"
    done <<< "$users"

    echo -e "\nBenchmark completed. Results saved to $OUTPUT_FILE"
}

main
