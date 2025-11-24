#!/bin/bash

# Start the test in the background
./gradlew :sirix-core:test --tests "io.sirix.axis.concurrent.ConcurrentAxisTest.testConcurrent" &
TEST_PID=$!

# Wait 5 seconds for the test to start and hang
sleep 5

# Find the Java process for the test
JAVA_PID=$(jps | grep GradleWorkerMain | awk '{print $1}')

if [ -n "$JAVA_PID" ]; then
    echo "==== Thread Dump at 5 seconds ===="
    jstack $JAVA_PID | grep -A 30 "pool-\|ForkJoin\|Test worker\|executor"
    
    sleep 5
    
    echo ""
    echo "==== Thread Dump at 10 seconds ===="
    jstack $JAVA_PID | grep -A 30 "pool-\|ForkJoin\|Test worker\|executor"
fi

# Kill the test
kill $TEST_PID 2>/dev/null
wait $TEST_PID 2>/dev/null

echo "Test terminated"

