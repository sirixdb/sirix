#!/bin/bash

# Fix missing Bytes imports in all Java files
echo "Adding missing Bytes imports..."
find bundles/sirix-core/src -name "*.java" -exec grep -l "Bytes\.elasticHeapByteBuffer" {} \; | while read file; do
    if ! grep -q "import io.sirix.node.Bytes" "$file"; then
        echo "Adding import to $file"
        if grep -q "import io.sirix.node.BytesOut;" "$file"; then
            sed -i '/import io\.sirix\.node\.BytesOut;/a import io.sirix.node.Bytes;' "$file"
        else
            # Add import after package declaration
            sed -i '/^package /a\\nimport io.sirix.node.Bytes;' "$file"
        fi
    fi
done

# Fix BytesOut to BytesIn conversion issues in test files
echo "Fixing BytesOut/BytesIn type mismatches..."
find bundles/sirix-core/src/test -name "*.java" -exec grep -l "\.deserialize(data," {} \; | while read file; do
    echo "Fixing BytesOut/BytesIn in $file"
    sed -i 's/\.deserialize(data,/\.deserialize(data.asBytesIn(),/g' "$file"
done

echo "Done fixing Bytes issues!"