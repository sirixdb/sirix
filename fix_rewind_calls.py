#!/usr/bin/env python3

import os
import re

def fix_rewind_calls(file_path):
    """Fix rewind() method calls by adding ByteBuffer cast"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Pattern to match bytes.underlyingObject().rewind()
        pattern = r'bytes\.underlyingObject\(\)\.rewind\(\)'
        replacement = r'((java.nio.ByteBuffer) bytes.underlyingObject()).rewind()'
        
        if re.search(pattern, content):
            new_content = re.sub(pattern, replacement, content)
            
            with open(file_path, 'w') as f:
                f.write(new_content)
            return True
        
    except Exception as e:
        print(f"Error fixing {file_path}: {e}")
    
    return False

def main():
    # Find all Java files in the project
    java_files = []
    for root, dirs, files in os.walk('/home/johannes/IdeaProjects/sirix/bundles/sirix-core/src'):
        for file in files:
            if file.endswith('.java'):
                java_files.append(os.path.join(root, file))
    
    fixed_count = 0
    for java_file in java_files:
        if fix_rewind_calls(java_file):
            print(f"Fixed rewind calls: {java_file}")
            fixed_count += 1
    
    print(f"Fixed rewind calls in {fixed_count} files")

if __name__ == "__main__":
    main()