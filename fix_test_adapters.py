#!/usr/bin/env python3

import os
import re
import glob

def fix_test_file(file_path):
    """Fix serialization/deserialization calls in a test file"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Fix serialize calls: wrap data parameter with ChronicleBytesOutAdapter
        content = re.sub(
            r'\.serialize\(data,',
            '.serialize(new ChronicleBytesOutAdapter(data),',
            content
        )
        
        # Fix deserialize calls: wrap data parameter with ChronicleBytesInAdapter  
        content = re.sub(
            r'\.deserialize\(data,',
            '.deserialize(new ChronicleBytesInAdapter(data),',
            content
        )
        
        with open(file_path, 'w') as f:
            f.write(content)
            
        print(f"Fixed: {file_path}")
        return True
        
    except Exception as e:
        print(f"Error fixing {file_path}: {e}")
        return False

def main():
    # Find all test files in the node package
    test_files = glob.glob('/home/johannes/IdeaProjects/sirix/bundles/sirix-core/src/test/java/io/sirix/node/**/*Test.java', recursive=True)
    
    fixed_count = 0
    for test_file in test_files:
        if fix_test_file(test_file):
            fixed_count += 1
    
    print(f"\nFixed {fixed_count} test files")

if __name__ == "__main__":
    main()