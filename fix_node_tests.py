#!/usr/bin/env python3
"""Fix node tests to use deserialize() instead of manual segment creation"""
import re
import os

def fix_test_file(filepath):
    """Fix a single test file"""
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Extract node type from filename
    basename = os.path.basename(filepath)
    if 'BooleanNodeTest' in basename:
        node_class = 'BooleanNode'
        node_kind = 'BOOLEAN_VALUE'
    elif 'NumberNodeTest' in basename and 'ObjectNumber' not in basename:
        node_class = 'NumberNode'
        node_kind = 'NUMBER_VALUE'
    elif 'ObjectNumberNodeTest' in basename:
        node_class = 'ObjectNumberNode'
        node_kind = 'OBJECT_NUMBER_VALUE'
    elif 'ObjectNodeTest' in basename:
        node_class = 'ObjectNode'
        node_kind = 'OBJECT'
    elif 'ArrayNodeTest' in basename:
        node_class = 'ArrayNode'
        node_kind = 'ARRAY'
    elif 'StringNodeTest' in basename and 'ObjectString' not in basename:
        node_class = 'StringNode'
        node_kind = 'STRING_VALUE'
    elif 'ObjectStringNodeTest' in basename:
        node_class = 'ObjectStringNode'
        node_kind = 'OBJECT_STRING_VALUE'
    elif 'ObjectBooleanNodeTest' in basename:
        node_class = 'ObjectBooleanNode'
        node_kind = 'OBJECT_BOOLEAN_VALUE'
    elif 'NullNodeTest' in basename and 'ObjectNull' not in basename:
        node_class = 'NullNode'
        node_kind = 'NULL_VALUE'
    elif 'ObjectNullNodeTest' in basename:
        node_class = 'ObjectNullNode'
        node_kind = 'OBJECT_NULL_VALUE'
    elif 'ObjectKeyNodeTest' in basename:
        node_class = 'ObjectKeyNode'
        node_kind = 'OBJECT_KEY'
    else:
        print(f"Skipping {basename} - unknown node type")
        return False
    
    # Pattern to find manual segment creation
    pattern = r'(var segment = \(MemorySegment\) data\.asBytesIn\(\)\.getUnderlying\(\);.*?)(final ' + node_class + r' node = new ' + node_class + r'\(segment, \d+L,.*?\);.*?)(var (?:hash)?[Bb]ytes = Bytes\.elasticHeapByteBuffer\(\);.*?node\.setHash\(node\.computeHash\((?:hash)?[Bb]ytes\)\);.*?)(check\(node\);)'
    
    # Replacement  
    replacement = rf'// Deserialize to create node\n    final {node_class} node = ({node_class}) NodeKind.{node_kind}.deserialize(data.asBytesIn(),\n                                                                               13L,\n                                                                               null,\n                                                                               pageTrx.getResourceSession()\n                                                                                      .getResourceConfig());\n    \4'
    
    content_new = re.sub(pattern, replacement, content, flags=re.DOTALL)
    
    if content_new == content:
        print(f"No changes for {basename}")
        return False
    
    with open(filepath, 'w') as f:
        f.write(content_new)
    
    print(f"Fixed {basename}")
    return True

# Fix all JSON node tests
test_dir = '/home/johannes/IdeaProjects/sirix/bundles/sirix-core/src/test/java/io/sirix/node/json'
test_files = [
    'BooleanNodeTest.java',
    'NumberNodeTest.java',
    'ObjectNumberNodeTest.java',
    'ObjectNodeTest.java',
    'ArrayNodeTest.java',
    'StringNodeTest.java',
    'ObjectStringNodeTest.java',
    'ObjectBooleanNodeTest.java',
    'NullNodeTest.java',
    'ObjectNullNodeTest.java',
]

for test_file in test_files:
    filepath = os.path.join(test_dir, test_file)
    if os.path.exists(filepath):
        fix_test_file(filepath)
    else:
        print(f"File not found: {filepath}")

print("Done!")
