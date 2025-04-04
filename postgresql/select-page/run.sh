#!/bin/bash

# Configuration
DB_NAME="test"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"

OUTPUT_FILE="results.csv"
TOP_USERS_LIMIT=10000
BATCH_SIZE=100

initialize_benchmark() {
    echo "user_address,transaction_count,batch_number,batch_size,batch_read_time_ms" > "$OUTPUT_FILE"
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
    done
}

process_single_batch() {
    local user_address="$1"
    local transaction_count="$2"
    local batch_num="$3"
    
    local offset=$(( (batch_num - 1) * BATCH_SIZE ))
    local limit=$(( batch_num < total_batches ? BATCH_SIZE : transaction_count % BATCH_SIZE ))
    [ $limit -eq 0 ] && limit=$BATCH_SIZE
    
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
    local users=$(get_top_users)
    
    while IFS='|' read -r user_address transaction_count; do
        user_address=$(trim_whitespace "$user_address")
        transaction_count=$(trim_whitespace "$transaction_count")
        [ -z "$user_address" ] && continue
        
        process_user "$user_address" "$transaction_count"
    done <<< "$users"
    
    echo "Benchmark completed. Results saved to $OUTPUT_FILE"
}

main
