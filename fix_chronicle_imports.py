#!/usr/bin/env python3

import os
import re
import glob

def fix_chronicle_imports(file_path):
    """Replace Chronicle imports and method signatures with custom BytesIn/BytesOut interfaces"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        original_content = content
        
        # Replace Chronicle imports only
        content = re.sub(r'import net\.openhft\.chronicle\.bytes\.Bytes;', 'import io.sirix.node.BytesOut;', content)
        content = re.sub(r'import net\.openhft\.chronicle\.bytes\.BytesIn;', 'import io.sirix.node.BytesIn;', content)
        content = re.sub(r'import net\.openhft\.chronicle\.bytes\.BytesOut;', 'import io.sirix.node.BytesOut;', content)
        
        # Replace method signatures for computeHash only
        content = re.sub(r'computeHash\(Bytes<ByteBuffer> ([^)]+)\)', r'computeHash(BytesOut<?> \1)', content)
        content = re.sub(r'computeHash\(final Bytes<ByteBuffer> ([^)]+)\)', r'computeHash(final BytesOut<?> \1)', content)
        
        # Replace ALL Bytes<ByteBuffer> usage with BytesOut<?>
        content = re.sub(r'Bytes<ByteBuffer>', 'BytesOut<?>', content)
        content = re.sub(r'Bytes<byte\[\]>', 'BytesOut<?>', content)
        
        # Also replace plain Bytes usage (when used without generic type)
        content = re.sub(r'\bBytes\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=', r'BytesOut<?> \1 =', content)
        
        # Remove ByteBuffer import only if Bytes<ByteBuffer> was replaced and no other ByteBuffer usage exists
        if 'Bytes<ByteBuffer>' in original_content and 'Bytes<ByteBuffer>' not in content:
            # Check if ByteBuffer is used elsewhere (not in Bytes<ByteBuffer> context)
            remaining_content_without_generics = re.sub(r'BytesOut<\?>', '', content)
            if 'ByteBuffer' not in remaining_content_without_generics:
                content = re.sub(r'import java\.nio\.ByteBuffer;\n?', '', content)
        
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    # Find all Java source files in the project
    source_files = []
    source_dirs = [
        '/home/johannes/IdeaProjects/sirix/bundles/sirix-core/src/main/java',
        '/home/johannes/IdeaProjects/sirix/bundles/sirix-core/src/test/java'
    ]
    
    for source_dir in source_dirs:
        if os.path.exists(source_dir):
            source_files.extend(glob.glob(f'{source_dir}/**/*.java', recursive=True))
    
    fixed_count = 0
    for file_path in source_files:
        if fix_chronicle_imports(file_path):
            print(f"Fixed: {file_path}")
            fixed_count += 1
    
    print(f"\nFixed Chronicle imports in {fixed_count} files")

if __name__ == "__main__":
    main()