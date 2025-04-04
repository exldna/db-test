#!/bin/bash

# Configuration
DB_NAME="test"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"

OUTPUT_FILE="results.csv"

TOP_USERS_LIMIT=10
BATCH_SIZE=1000

# Progress tracking
TOTAL_BATCHES=0
PROCESSED_BATCHES=0

initialize_benchmark() {
    echo "user_address,transaction_count,batch_number,batch_size,batch_read_time_ms" > "$OUTPUT_FILE"
}

calculate_total_batches() {
    local users=$(get_top_users)
    TOTAL_BATCHES=0
    
    while IFS='|' read -r _ transaction_count; do
        transaction_count=$(trim_whitespace "$transaction_count")
        [ -z "$transaction_count" ] && continue
        
        local batches=$(( (transaction_count + BATCH_SIZE - 1) / BATCH_SIZE ))
        TOTAL_BATCHES=$((TOTAL_BATCHES + batches))
    done <<< "$users"
    
    echo "$users"  # Return users data for processing
}

show_progress() {
    local current=$1
    local total=$2
    local width=50
    local percent=$((current * 100 / total))
    local completed=$((current * width / total))
    
    printf "\rProgress: [%-${width}s] %d%% (%d/%d batches)" \
           "$(printf '#%.0s' $(seq 1 $completed))" \
           "$percent" \
           "$current" \
           "$total"
}

get_top_users() {
    local query="
    SELECT user_address, count(*) AS transaction_count 
    FROM user_transactions 
    GROUP BY user_address 
    ORDER BY transaction_count DESC 
    LIMIT $TOP_USERS_LIMIT;"
    
    execute_sql_query "$query"
}

process_user() {
    local user_address="$1"
    local transaction_count="$2"
    
    local total_batches=$(( (transaction_count + BATCH_SIZE - 1) / BATCH_SIZE ))
    
    for ((batch_num=1; batch_num<=total_batches; batch_num++)); do
        process_single_batch "$user_address" "$transaction_count" "$batch_num"
        PROCESSED_BATCHES=$((PROCESSED_BATCHES + 1))
        show_progress "$PROCESSED_BATCHES" "$TOTAL_BATCHES"
    done
}

process_single_batch() {
    local user_address="$1"
    local transaction_count="$2"
    local batch_num="$3"
    
    local offset=$(( (batch_num - 1) * BATCH_SIZE ))
    local limit=$(( batch_num * BATCH_SIZE <= transaction_count ? BATCH_SIZE : transaction_count % BATCH_SIZE ))
    
    local query="
    SELECT transaction_timestamp 
    FROM (
        SELECT transaction_timestamp, 
               ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) as rn
        FROM user_transactions 
        WHERE user_address = '$user_address'
    ) sub 
    WHERE rn > $offset AND rn <= $((offset + limit));"
    
    local start_time=$(date +%s%3N)
    execute_sql_query "$query" >/dev/null
    local end_time=$(date +%s%3N)
    
    save_result "$user_address" "$transaction_count" "$batch_num" "$limit" "$((end_time - start_time))"
}

execute_sql_query() {
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$1" -q -t -A -F'|'
}

save_result() {
    echo "$1,$2,$3,$4,$5" >> "$OUTPUT_FILE"
}

trim_whitespace() {
    local str="$1"
    echo "$str" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

main() {
    initialize_benchmark
    local users=$(calculate_total_batches)
    
    echo "Total batches to process: $TOTAL_BATCHES"
    
    while IFS='|' read -r user_address transaction_count; do
        user_address=$(trim_whitespace "$user_address")
        transaction_count=$(trim_whitespace "$transaction_count")
        [ -z "$user_address" ] && continue
        
        process_user "$user_address" "$transaction_count"
    done <<< "$users"
    
    echo -e "\nBenchmark completed. Results saved to $OUTPUT_FILE"
}

main