#!/bin/bash

# Configuration
readonly DB_NAME="test"
readonly DB_USER="postgres"
readonly DB_HOST="localhost"
readonly DB_PORT="5432"

readonly OUTPUT_FILE="results.csv"
readonly TOP_USERS_LIMIT=100

main() {
    initialize_output_file
    local top_users=$(get_top_users)
    benchmark_users_sequential_read "$top_users"
    echo "Benchmark completed. Results saved to $OUTPUT_FILE"
}

initialize_output_file() {
    echo "user_address,transaction_count,read_all_time_ms" > "$OUTPUT_FILE"
}

get_top_users() {
    echo "Identifying top $TOP_USERS_LIMIT users by transaction count..."
    local query="
        SELECT user_address, count(*) AS transaction_count
        FROM user_transactions
        GROUP BY user_address
        ORDER BY transaction_count DESC
        LIMIT $TOP_USERS_LIMIT;"
    
    execute_sql_query "$query"
}

benchmark_users_sequential_read() {
    local users="$1"
    while IFS='|' read -r user_address transaction_count; do
        user_address=$(trim "$user_address")
        transaction_count=$(trim "$transaction_count")
        [ -z "$user_address" ] && continue
        
        measure_sequential_read "$user_address" "$transaction_count"
    done <<< "$users"
}

measure_sequential_read() {
    local user_address="$1"
    local transaction_count="$2"
    
    echo "Benchmarking sequential read for user: $user_address with $transaction_count transactions..."
    
    local query="
        SELECT transaction_timestamp
        FROM user_transactions
        WHERE user_address = '$user_address'
        ORDER BY transaction_timestamp ASC;"
    
    local start_time=$(date +%s%3N)
    execute_sql_query "$query" >/dev/null
    local end_time=$(date +%s%3N)
    local execution_time=$((end_time - start_time))
    
    save_results "$user_address" "$transaction_count" "$execution_time"
}

execute_sql_query() {
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$1" -q -t -A -F'|'
}

trim() {
    local str="$1"
    str="${str#"${str%%[![:space:]]*}"}"  # Remove leading whitespace
    str="${str%"${str##*[![:space:]]}"}"  # Remove trailing whitespace
    echo "$str"
}

save_results() {
    printf '"%s",%d,%d\n' "$1" "$2" "$3" >> "$OUTPUT_FILE"
}

# Run main function
main
