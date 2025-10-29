#!/bin/bash

# Script to run a test with PATH_SUMMARY diagnostics enabled

cd /home/johannes/IdeaProjects/sirix

echo "Running VersioningTest with PATH_SUMMARY diagnostics..."
echo "=============================================="
echo ""

./gradlew :sirix-core:test --tests "*VersioningTest.testFull1" \
  -Dtest.single=VersioningTest \
  --no-build-cache \
  --rerun-tasks \
  2>&1 | tee path-summary-diagnostic-output.log

echo ""
echo "=============================================="
echo "Test complete. Extracting PATH_SUMMARY diagnostic messages..."
echo ""

grep -E "\[PATH_SUMMARY-" path-summary-diagnostic-output.log | head -50

echo ""
echo "Full diagnostic output saved to: path-summary-diagnostic-output.log"

