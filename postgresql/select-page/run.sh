#!/bin/bash

# Configuration
DB_NAME="test"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
ITERATIONS=5
BATCH_SIZE=100

# Function to run a query and measure execution time
run_query() {
    local query=$1
    local test_name=$2
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

# Function to test batch processing performance
test_batch_processing() {
    echo -e "\n=== Testing Batch Processing Performance ==="
    
    # First get the top user
    top_user=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT user_address FROM user_transactions GROUP BY user_address ORDER BY count(*) DESC LIMIT 1;")
    
    if [ -z "$top_user" ]; then
        echo "Error: No top user found. Make sure the database has data."
        exit 1
    fi
    
    echo "Top user: $top_user"
    
    # Get total transaction count for the top user
    total_count=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT count(*) FROM user_transactions WHERE user_address = '$top_user';" | tr -d '[:space:]')
    
    echo "Total transactions for top user: $total_count"
    
    if [ "$total_count" -lt "$BATCH_SIZE" ]; then
        echo "Error: Not enough transactions for batch size $BATCH_SIZE"
        exit 1
    fi
    
    # Test random batch selection and time difference calculation
    total_time=0
    for ((i=1; i<=$ITERATIONS; i++)); do
        echo "Iteration $i/$ITERATIONS"
        
        # Generate random offset
        max_offset=$(( total_count - BATCH_SIZE ))
        random_offset=$(( RANDOM % max_offset ))
        
        start_time=$(date +%s%N)
        # Query to get batch and calculate time differences
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
            WITH batch AS (
                SELECT transaction_timestamp
                FROM (
                    SELECT transaction_timestamp,
                           ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) as rn
                    FROM user_transactions
                    WHERE user_address = '$top_user'
                ) sub
                WHERE rn >= $random_offset AND rn < $random_offset + $BATCH_SIZE
                ORDER BY transaction_timestamp
            )
            SELECT EXTRACT(EPOCH FROM (t2.transaction_timestamp - t1.transaction_timestamp)) as time_diff_seconds
            FROM batch t1
            JOIN batch t2 ON t2.rn = t1.rn + 1
            WHERE t2.rn <= $random_offset + $BATCH_SIZE;" > /dev/null
        
        end_time=$(date +%s%N)
        duration=$(( (end_time - start_time) / 1000000 ))
        total_time=$(( total_time + duration ))
        
        echo "Iteration $i time: $duration ms"
    done
    
    avg_time=$(( total_time / ITERATIONS ))
    echo -e "\nAverage time for batch processing: $avg_time ms"
}

# Main benchmark tests

# 1. Test finding the top user
run_query "SELECT user_address, count(*) AS transaction_count FROM user_transactions GROUP BY user_address ORDER BY transaction_count DESC LIMIT 1;" "Find top user with LIMIT"

run_query "SELECT user_address, count(*) AS transaction_count FROM user_transactions GROUP BY user_address HAVING count(*) = (SELECT max(count(*)) FROM user_transactions GROUP BY user_address);" "Find all top users with subquery"

# 2. Test getting ordered transactions for top user
top_user=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
    SELECT user_address FROM user_transactions GROUP BY user_address ORDER BY count(*) DESC LIMIT 1;" | tr -d '[:space:]')

if [ -n "$top_user" ]; then
    run_query "SELECT transaction_timestamp FROM user_transactions WHERE user_address = '$top_user' ORDER BY transaction_timestamp ASC;" "Get ordered transactions for top user"
else
    echo "Skipping ordered transactions test - no top user found"
fi

# 3. Test batch processing performance
test_batch_processing

# 4. Test index usage (this assumes indexes exist)
run_query "SELECT * FROM user_transactions WHERE user_address = '$top_user' ORDER BY transaction_timestamp ASC LIMIT 100;" "Query with potential index usage"

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