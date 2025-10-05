#!/bin/bash
# Fix XML node tests by adding Bytes import and fixing deserialize calls

cd /home/johannes/IdeaProjects/sirix

XML_TESTS=(
  "bundles/sirix-core/src/test/java/io/sirix/node/xml/AttributeNodeTest.java"
  "bundles/sirix-core/src/test/java/io/sirix/node/xml/DocumentRootNodeTest.java"
  "bundles/sirix-core/src/test/java/io/sirix/node/xml/PINodeTest.java"
  "bundles/sirix-core/src/test/java/io/sirix/node/xml/ElementNodeTest.java"
  "bundles/sirix-core/src/test/java/io/sirix/node/xml/TextNodeTest.java"
  "bundles/sirix-core/src/test/java/io/sirix/node/xml/CommentNodeTest.java"
  "bundles/sirix-core/src/test/java/io/sirix/node/xml/NamespaceNodeTest.java"
)

for test in "${XML_TESTS[@]}"; do
  echo "Fixing $test..."
  
  # Add Bytes import if not present
  if ! grep -q "import io.sirix.node.Bytes;" "$test"; then
    sed -i '/import io.sirix.node.BytesOut;/a import io.sirix.node.Bytes;' "$test"
  fi
  
  # Fix deserialize calls: data, -> data.bytesForRead(),
  sed -i 's/\.deserialize(data,/.deserialize(data.bytesForRead(),/g' "$test"
  
  # Fix var bytes = Bytes -> var hashBytes = Bytes (to avoid conflict)
  sed -i 's/var bytes = Bytes\.elasticHeapByteBuffer();/var hashBytes = Bytes.elasticHeapByteBuffer();/g' "$test"
  sed -i 's/node\.setHash(node\.computeHash(bytes));/node.setHash(node.computeHash(hashBytes));/g' "$test"
  
  echo "  Done!"
done

echo "All XML tests fixed!"

