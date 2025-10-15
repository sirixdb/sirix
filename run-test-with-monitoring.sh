#!/bin/bash

echo "Running test with memory monitoring..."
echo "Press Ctrl+C to stop"
echo ""

# Start the test in background
./gradlew :sirix-core:test --tests 'JsonShredderTest.testShredderAndTraverseChicago' --rerun-tasks > /tmp/test-full.log 2>&1 &
TEST_PID=$!

echo "Test PID: $TEST_PID"
echo "Monitoring memory usage..."
echo ""

# Monitor and log
while kill -0 $TEST_PID 2>/dev/null; do
    # Get memory usage
    MEM_KB=$(ps -p $TEST_PID -o rss= 2>/dev/null || echo "0")
    MEM_MB=$((MEM_KB / 1024))
    
    # Get pool sizes if log exists
    if [ -f memory-leak-diagnostic.log ]; then
        ALLOCS=$(grep -c 'allocate().*segments remaining' memory-leak-diagnostic.log 2>/dev/null || echo "0")
        RELEASES=$(grep -c 'Segment returned to pool' memory-leak-diagnostic.log 2>/dev/null || echo "0")
        LEAKED=$((ALLOCS - RELEASES))
        
        printf "[%s] Memory: %5d MB | Allocs: %6d | Releases: %6d | Leaked: %5d\n" \
            "$(date +%H:%M:%S)" "$MEM_MB" "$ALLOCS" "$RELEASES" "$LEAKED"
    else
        printf "[%s] Memory: %5d MB | Waiting for log file...\n" \
            "$(date +%H:%M:%S)" "$MEM_MB"
    fi
    
    sleep 5
done

wait $TEST_PID
EXIT_CODE=$?

echo ""
echo "Test completed with exit code: $EXIT_CODE"
echo ""

if [ -f memory-leak-diagnostic.log ]; then
    echo "Final statistics:"
    echo "  Allocations: $(grep -c 'allocate().*segments remaining' memory-leak-diagnostic.log)"
    echo "  Releases: $(grep -c 'Segment returned to pool' memory-leak-diagnostic.log)"
    echo "  Net: $(($(grep -c 'allocate().*segments remaining' memory-leak-diagnostic.log) - $(grep -c 'Segment returned to pool' memory-leak-diagnostic.log)))"
    echo ""
    echo "Log file: memory-leak-diagnostic.log ($(wc -l < memory-leak-diagnostic.log) lines)"
else
    echo "No diagnostic log found"
fi




