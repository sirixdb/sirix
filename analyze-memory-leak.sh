#!/bin/bash

# Memory Leak Analysis Script
# Run this after your test completes to analyze the diagnostic log

LOG_FILE="memory-leak-diagnostic.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "Error: $LOG_FILE not found. Run your test first."
    exit 1
fi

echo "========================================="
echo "Memory Leak Diagnostic Analysis"
echo "========================================="
echo ""

# Count allocations and releases
echo "1. ALLOCATION vs RELEASE counts:"
echo "   Allocations: $(grep -c 'allocate().*segments remaining' $LOG_FILE)"
echo "   Releases:    $(grep -c 'release().*pool now has' $LOG_FILE)"
echo ""

# Show pool size progression for pool 4 (65536 bytes)
echo "2. Pool 4 (65536 bytes) size progression (first 20):"
grep 'pool 4 now has' $LOG_FILE | head -20
echo ""

echo "3. Pool 4 (65536 bytes) size progression (last 20):"
grep 'pool 4 now has' $LOG_FILE | tail -20
echo ""

# Show pool size progression for pool 5 (131072 bytes)
echo "4. Pool 5 (131072 bytes) size progression (first 20):"
grep 'pool 5 now has' $LOG_FILE | head -20
echo ""

echo "5. Pool 5 (131072 bytes) size progression (last 20):"
grep 'pool 5 now has' $LOG_FILE | tail -20
echo ""

# Find pages that were borrowed but never returned
echo "6. Checking for pages returned multiple times with different sizes:"
grep 'returnSegmentsToAllocator called for page' $LOG_FILE | \
    awk '{print $7, $9, $13, $15}' | \
    sort | uniq -c | sort -rn | head -20
echo ""

# Count unique pages
echo "7. Total unique pages that returned segments:"
grep 'Successfully returned segments for page' $LOG_FILE | \
    awk '{print $6}' | sort -u | wc -l
echo ""

# Check for errors
echo "8. Errors (if any):"
grep -i error $LOG_FILE | head -10
echo ""

# Show statistics sections
echo "9. Pool statistics from log:"
grep -A 10 "POOL STATISTICS" $LOG_FILE | tail -1
echo ""

echo "========================================="
echo "Analysis complete. Full log: $LOG_FILE"
echo "========================================="



