#!/bin/bash

# PostgreSQL Benchmark Script for Transaction Time Analysis

# Configuration
DB_NAME="test"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"

ITERATIONS=5
TARGET_BATCH_SIZE=100

run_query() {
    local query=$2
    local test_name=$1
    local total_time=0
    
    echo -e "\n=== Benchmarking: $test_name ==="
    
    for ((i=1; i<=$ITERATIONS; i++)); do
        echo "Iteration $i/$ITERATIONS"
        
        # Measure query time using EXPLAIN ANALYZE
        start_time=$(date +%s%N)
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "EXPLAIN ANALYZE $query" > /dev/null
        end_time=$(date +%s%N)
        
        # Calculate duration in milliseconds
        duration=$(( (end_time - start_time) / 1000000 ))
        total_time=$(( total_time + duration ))
        
        echo "Iteration $i time: $duration ms"
    done
    
    avg_time=$(( total_time / ITERATIONS ))
    echo -e "\nAverage time for $test_name: $avg_time ms"
}

test_batch_processing() {
    echo -e "\n=== Testing Batch Processing Performance ==="
    
    # First get the top user
    top_user=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        WITH user_counts AS (
            SELECT user_address, count(*) AS transaction_count 
            FROM user_transactions 
            GROUP BY user_address
        )
        SELECT user_address 
        FROM user_counts 
        WHERE transaction_count = (SELECT max(transaction_count) FROM user_counts) 
        LIMIT 1;")
    
    if [ -z "$top_user" ]; then
        echo "Error: No top user found. Make sure the database has data."
        exit 1
    fi
    
    echo "Top user: $top_user"
    
    # Get total transaction count for the top user
    total_count=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT count(*) FROM user_transactions WHERE user_address = '$top_user';" | tr -d '[:space:]')
    
    echo "Total transactions for top user: $total_count"
    
    # Dynamically adjust batch size based on available data
    if [ "$total_count" -lt "$TARGET_BATCH_SIZE" ]; then
        BATCH_SIZE=$((total_count / 2))
        if [ "$BATCH_SIZE" -lt 2 ]; then
            echo "Error: Not enough transactions (need at least 2 for time difference analysis)"
            exit 1
        fi
        echo "Adjusted batch size to $BATCH_SIZE (half of available transactions)"
    else
        BATCH_SIZE=$TARGET_BATCH_SIZE
    fi
    
    # Test random batch selection and time difference calculation
    total_time=0
    for ((i=1; i<=$ITERATIONS; i++)); do
        echo "Iteration $i/$ITERATIONS (Batch size: $BATCH_SIZE)"
        
        # Generate random offset
        max_offset=$(( total_count - BATCH_SIZE ))
        random_offset=$(( RANDOM % (max_offset + 1) ))  # +1 to include max_offset
        
        start_time=$(date +%s%N)
        # Query to get batch and calculate time differences
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
            WITH batch AS (
                SELECT transaction_timestamp, rn
                FROM (
                    SELECT transaction_timestamp,
                           ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) as rn
                    FROM user_transactions
                    WHERE user_address = '$top_user'
                ) sub
                WHERE rn > $random_offset AND rn <= $random_offset + $BATCH_SIZE
                ORDER BY transaction_timestamp
            )
            SELECT EXTRACT(EPOCH FROM (t2.transaction_timestamp - t1.transaction_timestamp)) as time_diff_seconds
            FROM batch t1
            JOIN batch t2 ON t2.rn = t1.rn + 1;" > /dev/null
        
        end_time=$(date +%s%N)
        duration=$(( (end_time - start_time) / 1000000 ))
        total_time=$(( total_time + duration ))
        
        echo "Iteration $i time: $duration ms"
    done
    
    avg_time=$(( total_time / ITERATIONS ))
    echo -e "\nAverage time for batch processing (size $BATCH_SIZE): $avg_time ms"
}

# Main benchmark tests

# 1. Test finding the top user
run_query "Find top user with LIMIT" "\
    SELECT user_address, count(*) AS transaction_count \
    FROM user_transactions \
    GROUP BY user_address \
    ORDER BY transaction_count \
    DESC LIMIT 1;"

run_query "Find all top users with CTE" "
    WITH user_counts AS (
        SELECT user_address, count(*) AS transaction_count
        FROM user_transactions
        GROUP BY user_address
    ) SELECT user_address
    FROM user_counts
    WHERE transaction_count = (
        SELECT max(transaction_count) FROM user_counts
    );"

# 2. Test getting ordered transactions for top user
top_user=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
    WITH user_counts AS (
        SELECT user_address, count(*) AS transaction_count 
        FROM user_transactions 
        GROUP BY user_address
    )
    SELECT user_address 
    FROM user_counts 
    WHERE transaction_count = (SELECT max(transaction_count) FROM user_counts) 
    LIMIT 1;" | tr -d '[:space:]')

if [ -n "$top_user" ]; then
    run_query "Get ordered transactions for top user" "
        SELECT transaction_timestamp
        FROM user_transactions
        WHERE user_address = '$top_user'
        ORDER BY transaction_timestamp ASC;"
else
    echo "Skipping ordered transactions test - no top user found"
fi

# 3. Test batch processing performance
test_batch_processing

# 4. Test index usage (this assumes indexes exist)
if [ -n "$top_user" ]; then
    run_query "Query with potential index usage" "
        SELECT *
        FROM user_transactions
        WHERE user_address = '$top_user'
        ORDER BY transaction_timestamp ASC LIMIT 100;"
fi

# 5. Test time difference calculations
if [ -n "$top_user" ]; then
    run_query "
        WITH sample_transactions AS (
            SELECT transaction_timestamp
            FROM user_transactions
            WHERE user_address = '$top_user'
            ORDER BY transaction_timestamp
            LIMIT 100
        )
        SELECT 
            EXTRACT(EPOCH FROM (t2.transaction_timestamp - t1.transaction_timestamp)) as time_diff_seconds
        FROM 
            (SELECT transaction_timestamp, ROW_NUMBER() OVER () as rn FROM sample_transactions) t1
        JOIN 
            (SELECT transaction_timestamp, ROW_NUMBER() OVER () as rn FROM sample_transactions) t2
        ON t2.rn = t1.rn + 1;" "Time difference calculation"
fi

echo -e "\nBenchmark completed"
