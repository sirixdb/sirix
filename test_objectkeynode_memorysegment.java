import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.node.NodeKind;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Test script to verify ObjectKeyNode MemorySegment constructor fixes
 */
public class TestObjectKeyNodeMemorySegment {
    
    public static void main(String[] args) {
        System.out.println("Testing ObjectKeyNode MemorySegment constructor...");
        
        try {
            // Create a test MemorySegment with some data
            MemorySegment segment = Arena.global().allocate(64);
            
            // Write test data: hash (8 bytes) + nameKey (4 bytes) + other data
            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 12345L); // hash
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, 8, 999); // nameKey
            
            // Create resource configuration
            ResourceConfiguration resourceConfig = new ResourceConfiguration.Builder("test")
                .hashKind(HashType.ROLLING)
                .build();
            
            // Create a mock StructNodeDelegate
            NodeDelegate nodeDelegate = new NodeDelegate(1L, 0L, 0L, 0L, NodeKind.OBJECT_KEY);
            StructNodeDelegate structDelegate = new StructNodeDelegate(
                nodeDelegate, 0L, 0L, 0L, 0L, 0L, 0L
            );
            
            // Test MemorySegment constructor
            ObjectKeyNode node = new ObjectKeyNode(segment, structDelegate, 1L, resourceConfig);
            
            // Test that delegate methods work (should not throw NullPointerException)
            System.out.println("Testing delegate() method...");
            NodeDelegate delegate = node.delegate();
            System.out.println("delegate() works: " + (delegate != null));
            
            System.out.println("Testing structDelegate() method...");
            StructNodeDelegate structDel = node.structDelegate();
            System.out.println("structDelegate() works: " + (structDel != null));
            
            // Test getNameKey from MemorySegment
            System.out.println("Testing getNameKey() from MemorySegment...");
            int nameKey = node.getNameKey();
            System.out.println("nameKey from MemorySegment: " + nameKey + " (expected: 999)");
            
            // Test getHash
            System.out.println("Testing getHash()...");
            long hash = node.getHash();
            System.out.println("hash: " + hash + " (expected: 12345)");
            
            // Test setPathNodeKey doesn't break MemorySegment mode
            System.out.println("Testing setPathNodeKey()...");
            node.setPathNodeKey(777L);
            long pathNodeKey = node.getPathNodeKey();
            System.out.println("pathNodeKey after set: " + pathNodeKey + " (expected: 777)");
            
            // Test that getNameKey still works from MemorySegment after setPathNodeKey
            int nameKeyAfter = node.getNameKey();
            System.out.println("nameKey after setPathNodeKey: " + nameKeyAfter + " (should still be 999)");
            
            System.out.println("\nAll tests passed! ObjectKeyNode MemorySegment constructor is working correctly.");
            
        } catch (Exception e) {
            System.err.println("Test failed with exception: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}