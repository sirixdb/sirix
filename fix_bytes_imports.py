#!/usr/bin/env python3

import os
import re
import glob

def fix_bytes_imports(file_path):
    """Add missing import for Bytes class"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Check if Bytes is used but import is missing
        uses_bytes = 'Bytes.elasticHeapByteBuffer' in content
        has_import = 'import io.sirix.node.Bytes' in content
        
        if not uses_bytes or has_import:
            return False
        
        # Find the import section (after package declaration, before class declaration)
        lines = content.split('\n')
        import_insert_idx = -1
        
        for i, line in enumerate(lines):
            if line.startswith('import '):
                import_insert_idx = i
            elif line.startswith('public class') or line.startswith('final class'):
                break
        
        if import_insert_idx == -1:
            # No imports found, insert after package declaration
            for i, line in enumerate(lines):
                if line.startswith('package '):
                    import_insert_idx = i + 1
                    break
        
        if import_insert_idx != -1:
            # Insert import after the last existing import
            lines.insert(import_insert_idx + 1, 'import io.sirix.node.Bytes;')
            
            with open(file_path, 'w') as f:
                f.write('\n'.join(lines))
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
        if fix_bytes_imports(java_file):
            print(f"Fixed Bytes import: {java_file}")
            fixed_count += 1
    
    print(f"Fixed Bytes imports in {fixed_count} files")

if __name__ == "__main__":
    main()