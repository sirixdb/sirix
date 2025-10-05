#!/usr/bin/env python3
import os
import re

def fix_test_imports(file_path):
    """Add missing import statements for Chronicle adapter classes"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Check if adapters are used but imports are missing
    uses_out_adapter = 'ChronicleBytesOutAdapter' in content
    uses_in_adapter = 'ChronicleBytesInAdapter' in content
    has_out_import = 'import io.sirix.node.ChronicleBytesOutAdapter' in content
    has_in_import = 'import io.sirix.node.ChronicleBytesInAdapter' in content
    
    if not uses_out_adapter and not uses_in_adapter:
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
    
    imports_to_add = []
    if uses_out_adapter and not has_out_import:
        imports_to_add.append('import io.sirix.node.ChronicleBytesOutAdapter;')
    if uses_in_adapter and not has_in_import:
        imports_to_add.append('import io.sirix.node.ChronicleBytesInAdapter;')
    
    if imports_to_add:
        # Insert imports after the last existing import
        for import_stmt in reversed(imports_to_add):
            lines.insert(import_insert_idx + 1, import_stmt)
        
        with open(file_path, 'w') as f:
            f.write('\n'.join(lines))
        return True
    
    return False

def main():
    test_dir = '/home/johannes/IdeaProjects/sirix/bundles/sirix-core/src/test/java'
    fixed_count = 0
    
    for root, dirs, files in os.walk(test_dir):
        for file in files:
            if file.endswith('.java'):
                file_path = os.path.join(root, file)
                if fix_test_imports(file_path):
                    print(f"Fixed imports: {file_path}")
                    fixed_count += 1
    
    print(f"Fixed imports in {fixed_count} test files")

if __name__ == '__main__':
    main()