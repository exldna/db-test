#!/bin/bash

# Configuration
readonly DB_NAME="test"
readonly DB_USER="postgres"
readonly DB_HOST="localhost"
readonly DB_PORT="5432"

readonly BATCH_SIZE=100
readonly NUM_ITERATIONS=5
readonly NUM_USERS_TO_TEST=100
readonly OUTPUT_FILE="results.csv"

run_benchmark() {
    create_output_file
    local top_users=$(identify_top_users)
    benchmark_users "$top_users"
    echo "Benchmark completed. Results saved to $OUTPUT_FILE"
}

create_output_file() {
    echo "user_address,transaction_count,avg_query_time_ms,min_query_time_ms,max_query_time_ms" > "$OUTPUT_FILE"
}

identify_top_users() {
    echo "Identifying top $NUM_USERS_TO_TEST users by transaction count..."
    local query="
    SELECT user_address, count(*) AS transaction_count
    FROM user_transactions
    GROUP BY user_address
    ORDER BY transaction_count DESC
    LIMIT $NUM_USERS_TO_TEST;"
    
    execute_query "$query"
}

benchmark_users() {
    local users="$1"
    while read -r user_address transaction_count; do
        [ -z "$user_address" ] && continue
        benchmark_single_user "$user_address" "$transaction_count"
    done <<< "$users"
}

benchmark_single_user() {
    local user_address="$1"
    local transaction_count="$2"
    
    echo "Benchmarking user: $user_address with $transaction_count transactions..."
    
    local total_time=0
    local min_time=999999
    local max_time=0
    
    for ((i=1; i<=NUM_ITERATIONS; i++)); do
        local query_time=$(benchmark_single_iteration "$user_address" "$transaction_count")
        
        total_time=$((total_time + query_time))
        ((query_time < min_time)) && min_time=$query_time
        ((query_time > max_time)) && max_time=$query_time
        
        echo "  Iteration $i: $query_time ms"
    done
    
    local avg_time=$((total_time / NUM_ITERATIONS))
    record_results "$user_address" "$transaction_count" "$avg_time" "$min_time" "$max_time"
}

benchmark_single_iteration() {
    local user_address="$1"
    local transaction_count="$2"
    
    local offset=$(calculate_random_offset "$transaction_count")
    local end_pos=$((offset + BATCH_SIZE))
    
    # Формируем запрос с правильными параметрами
    local query=$(cat <<EOF
    SELECT transaction_timestamp
    FROM (
        SELECT transaction_timestamp,
               ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) as rn
        FROM user_transactions
        WHERE user_address = '$user_address'
    ) sub
    WHERE rn >= $offset AND rn < $end_pos;
EOF
    )
    
    execute_query_with_timing "$query"
}

calculate_random_offset() {
    local transaction_count="$1"
    local max_offset=$((transaction_count - BATCH_SIZE))
    (( max_offset <= 0 )) && echo 0 || echo $((RANDOM % max_offset))
}

execute_query() {
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$1" -q -t
}

execute_query_with_timing() {
    local start_time=$(date +%s%3N)
    execute_query "$1" >/dev/null
    local end_time=$(date +%s%3N)
    echo $((end_time - start_time))
}

record_results() {
    local user_address="$1"
    local transaction_count="$2"
    local avg_time="$3"
    local min_time="$4"
    local max_time="$5"
    
    echo "\"$user_address\",$transaction_count,$avg_time,$min_time,$max_time" >> "$OUTPUT_FILE"
}

run_benchmark
