#!/bin/bash
# Helper script to run tests with leak tracking enabled and analyze results

set -e

cd "$(dirname "$0")"

echo "==================================="
echo "Page Leak Source Tracking Script"
echo "==================================="
echo

# Parse arguments
TEST_CLASS="${1:-io.sirix.diff.algorithm.FMSETest}"
OUTPUT_LOG="${2:-leak-trace-$(date +%Y%m%d-%H%M%S).log}"

echo "Test class: $TEST_CLASS"
echo "Output log: $OUTPUT_LOG"
echo

# Run test with debug enabled
echo "Running test with DEBUG_MEMORY_LEAKS enabled..."
echo "Command: ./gradlew :bundles:sirix-core:test --tests \"$TEST_CLASS\" -Dsirix.debug.memory.leaks=true"
echo

./gradlew :bundles:sirix-core:test --tests "$TEST_CLASS" \
  -Dsirix.debug.memory.leaks=true \
  > "$OUTPUT_LOG" 2>&1 || true

echo
echo "Test completed. Log saved to: $OUTPUT_LOG"
echo

# Analyze results
echo "==================================="
echo "Leak Analysis"
echo "==================================="
echo

# Count total leaks
TOTAL_LEAKS=$(grep -c "FINALIZER LEAK CAUGHT" "$OUTPUT_LOG" || echo "0")
echo "Total leaked pages: $TOTAL_LEAKS"
echo

if [ "$TOTAL_LEAKS" -eq "0" ]; then
    echo "✅ No leaks detected!"
    exit 0
fi

# Count leaks by index type
echo "Leaks by index type:"
grep "FINALIZER LEAK CAUGHT" "$OUTPUT_LOG" | \
  sed -E 's/.*Page [0-9]+ \(([A-Z_]+)\).*/\1/' | \
  sort | uniq -c | sort -rn
echo

# Count leaks by page key
echo "Top 10 leaked page keys:"
grep "FINALIZER LEAK CAUGHT" "$OUTPUT_LOG" | \
  sed -E 's/.*Page ([0-9]+) \(.*/\1/' | \
  sort -n | uniq -c | sort -rn | head -10
echo

# Analyze creation sources (if stack traces are present)
if grep -q "Created at:" "$OUTPUT_LOG"; then
    echo "Leaks by creation method:"
    grep "Created at:" -A 1 "$OUTPUT_LOG" | \
      grep -E "^\s+io.sirix" | \
      sed -E 's/^\s+([^(]+).*/\1/' | \
      sort | uniq -c | sort -rn | head -10
    echo
    
    echo "Detailed creation paths saved to: leak-paths.txt"
    grep -A 10 "FINALIZER LEAK CAUGHT" "$OUTPUT_LOG" | \
      grep -E "(FINALIZER LEAK CAUGHT|Created at:|io.sirix)" \
      > leak-paths.txt
    echo
else
    echo "⚠️  WARNING: No stack traces found in log!"
    echo "   This means DEBUG_MEMORY_LEAKS was not enabled."
    echo "   Re-run with: -Dsirix.debug.memory.leaks=true"
    echo
fi

# Check for guard protection issues
GUARD_PROTECTED=$(grep -c "GUARD PROTECTED" "$OUTPUT_LOG" || echo "0")
if [ "$GUARD_PROTECTED" -gt "0" ]; then
    echo "⚠️  Found $GUARD_PROTECTED guard protection warnings"
    echo "   (Pages still in use when cache tried to close them)"
    echo
fi

# Summary
echo "==================================="
echo "Summary"
echo "==================================="
echo "Total leaks: $TOTAL_LEAKS"
echo "Guard protected warnings: $GUARD_PROTECTED"
echo
echo "Full log: $OUTPUT_LOG"
if [ -f "leak-paths.txt" ]; then
    echo "Creation paths: leak-paths.txt"
fi
echo
echo "Review PAGE_LEAK_SOURCE_TRACKING.md for analysis guidance."


