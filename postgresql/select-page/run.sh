#!/bin/bash

# Configuration
readonly DB_NAME="test"
readonly DB_USER="postgres"
readonly DB_HOST="localhost"
readonly DB_PORT="5432"

readonly BATCH_SIZE=100
readonly NUM_ITERATIONS=5
readonly NUM_USERS_TO_TEST=3
readonly OUTPUT_FILE="results.csv"

main() {
    initialize_output_file
    local top_users=$(get_top_users)
    benchmark_all_users "$top_users"
    echo "Benchmark completed. Results saved to $OUTPUT_FILE"
}

initialize_output_file() {
    echo "user_address,transaction_count,avg_query_time_ms,min_query_time_ms,max_query_time_ms" > "$OUTPUT_FILE"
}

get_top_users() {
    echo "Identifying top $NUM_USERS_TO_TEST users by transaction count..."
    local query="SELECT user_address, count(*) AS transaction_count FROM user_transactions GROUP BY user_address ORDER BY transaction_count DESC LIMIT $NUM_USERS_TO_TEST;"
    execute_sql_query "$query"
}

benchmark_all_users() {
    local users="$1"
    while IFS='|' read -r user_address transaction_count; do
        user_address=$(trim "$user_address")
        transaction_count=$(trim "$transaction_count")
        [ -z "$user_address" ] && continue
        
        benchmark_user_transactions "$user_address" "$transaction_count"
    done <<< "$users"
}

benchmark_user_transactions() {
    local user_address="$1"
    local transaction_count="$2"
    
    echo "Benchmarking user: $user_address with $transaction_count transactions..."
    
    local total_time=0 min_time=999999 max_time=0
    
    for ((i=1; i<=NUM_ITERATIONS; i++)); do
        local query_time=$(measure_query_time "$user_address" "$transaction_count")
        
        total_time=$((total_time + query_time))
        [ $query_time -lt $min_time ] && min_time=$query_time
        [ $query_time -gt $max_time ] && max_time=$query_time
        
        echo "  Iteration $i: $query_time ms"
    done
    
    local avg_time=$((total_time / NUM_ITERATIONS))
    save_results "$user_address" "$transaction_count" "$avg_time" "$min_time" "$max_time"
}

measure_query_time() {
    local user_address="$1"
    local transaction_count="$2"
    
    local offset=$(calculate_offset "$transaction_count")
    local end_pos=$((offset + BATCH_SIZE))
    
    local query="SELECT transaction_timestamp FROM (
        SELECT transaction_timestamp, ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) as rn
        FROM user_transactions
        WHERE user_address = '$user_address'
    ) sub WHERE rn >= $offset AND rn < $end_pos;"
    
    measure_sql_query_time "$query"
}

calculate_offset() {
    local transaction_count="$1"
    local max_offset=$((transaction_count - BATCH_SIZE))
    [ $max_offset -le 0 ] && echo 0 || echo $((RANDOM % max_offset))
}

execute_sql_query() {
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$1" -q -t -A -F'|'
}

measure_sql_query_time() {
    local start_time=$(date +%s%3N)
    execute_sql_query "$1" >/dev/null
    local end_time=$(date +%s%3N)
    echo $((end_time - start_time))
}

trim() {
    local str="$1"
    str="${str#"${str%%[![:space:]]*}"}"  # Remove leading whitespace
    str="${str%"${str##*[![:space:]]}"}"  # Remove trailing whitespace
    echo "$str"
}

save_results() {
    printf '"%s",%d,%d,%d,%d\n' "$1" "$2" "$3" "$4" "$5" >> "$OUTPUT_FILE"
}

# Run main function
main