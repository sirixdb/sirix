#!/bin/bash
# Verification script to test if all accounting errors are fixed

echo "========================================="
echo "VERIFICATION SCRIPT - Testing All Fixes"
echo "========================================="
echo

echo "Step 1: Verify on latest commit..."
git log --oneline -1
echo

echo "Step 2: Clean build..."
./gradlew clean
echo

echo "Step 3: Build with latest code..."
./gradlew build -x test
echo

echo "Step 4: Test sirix-core VersioningTest..."
./gradlew :sirix-core:test --tests "*VersioningTest*" 2>&1 | tee /tmp/versioning-test.log | tail -20
ERROR_COUNT_1=$(grep -c "Physical memory accounting error" /tmp/versioning-test.log || echo "0")
echo
echo "VersioningTest accounting errors: $ERROR_COUNT_1"
echo

echo "Step 5: Test sirix-query XMark (xmark03)..."
./gradlew :sirix-query:test --tests "*SirixXMarkTest.xmark03*" 2>&1 | tee /tmp/xmark03-test.log | tail -20
ERROR_COUNT_2=$(grep -c "Physical memory accounting error" /tmp/xmark03-test.log || echo "0")
echo
echo "xmark03 accounting errors: $ERROR_COUNT_2"
echo

echo "Step 6: Test sirix-query XMark (xmark04)..."
./gradlew :sirix-query:test --tests "*SirixXMarkTest.xmark04*" 2>&1 | tee /tmp/xmark04-test.log | tail -20
ERROR_COUNT_3=$(grep -c "Physical memory accounting error" /tmp/xmark04-test.log || echo "0")
echo
echo "xmark04 accounting errors: $ERROR_COUNT_3"
echo

echo "========================================="
echo "FINAL RESULTS:"
echo "  VersioningTest errors: $ERROR_COUNT_1"
echo "  xmark03 errors: $ERROR_COUNT_2"
echo "  xmark04 errors: $ERROR_COUNT_3"
TOTAL=$((ERROR_COUNT_1 + ERROR_COUNT_2 + ERROR_COUNT_3))
echo "  TOTAL ERRORS: $TOTAL"
echo

if [ $TOTAL -eq 0 ]; then
    echo "✅ SUCCESS: ZERO accounting errors!"
    echo "✅ All fixes working correctly!"
else
    echo "❌ ERRORS DETECTED: $TOTAL accounting errors found"
    echo "Showing first few errors from xmark03:"
    grep "Physical memory accounting error" /tmp/xmark03-test.log | head -5
    echo
    echo "Showing first few errors from xmark04:"
    grep "Physical memory accounting error" /tmp/xmark04-test.log | head -5
fi
echo "========================================="

