#!/bin/bash
# Fix remaining test utilities by adding Bytes import

cd /home/johannes/IdeaProjects/sirix

TESTS=(
  "bundles/sirix-core/src/test/java/io/sirix/page/SerializationDeserializationTest.java"
  "bundles/sirix-core/src/test/java/io/sirix/page/PageTest.java"
  "bundles/sirix-core/src/test/java/io/sirix/io/IOTestHelper.java"
)

for test in "${TESTS[@]}"; do
  echo "Fixing $test..."
  
  # Add Bytes import if not present
  if ! grep -q "import io.sirix.node.Bytes;" "$test"; then
    # Find first io.sirix import and add after it
    sed -i '0,/^import io\.sirix\./ s/^import io\.sirix\./import io.sirix.node.Bytes;\nimport io.sirix./' "$test"
  fi
  
  echo "  Done!"
done

echo "All remaining tests fixed!"

