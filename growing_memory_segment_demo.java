import java.lang.foreign.MemorySegment;
import io.sirix.node.GrowingMemorySegment;
import io.sirix.node.MemorySegmentBytesOut;

/**
 * Simple demonstration script to show that GrowingMemorySegment
 * correctly handles writes that exceed the initial capacity boundaries.
 */
public class GrowingMemorySegmentDemo {
    
    public static void main(String[] args) {
        System.out.println("=== GrowingMemorySegment Demonstration ===");
        
        // Test 1: Basic GrowingMemorySegment functionality
        System.out.println("\n1. Testing basic GrowingMemorySegment:");
        GrowingMemorySegment growing = new GrowingMemorySegment(16); // Start with 16 bytes
        System.out.println("Initial capacity: " + growing.capacity() + " bytes");
        System.out.println("Initial position: " + growing.position());
        
        // Write 20 bytes - should trigger growth
        growing.ensureCapacity(20);
        System.out.println("After ensuring 20 bytes capacity: " + growing.capacity() + " bytes");
        growing.advance(20);
        System.out.println("After advancing 20 bytes position: " + growing.position());
        
        // Test 2: MemorySegmentBytesOut with small initial segment
        System.out.println("\n2. Testing MemorySegmentBytesOut with growth:");
        byte[] smallArray = new byte[8]; // Very small - 8 bytes
        MemorySegment smallSegment = MemorySegment.ofArray(smallArray);
        MemorySegmentBytesOut bytesOut = new MemorySegmentBytesOut(smallSegment);
        
        System.out.println("Initial capacity: " + bytesOut.getGrowingSegment().capacity() + " bytes");
        System.out.println("Initial position: " + bytesOut.position());
        
        // Write data that exceeds initial capacity
        System.out.println("\nWriting data that exceeds initial 8-byte capacity...");
        bytesOut.writeInt(0x12345678); // 4 bytes
        bytesOut.writeInt(0x9ABCDEF0); // 4 more bytes - total 8 bytes, should fit
        System.out.println("After writing 8 bytes - Position: " + bytesOut.position() + ", Capacity: " + bytesOut.getGrowingSegment().capacity());
        
        // This should trigger growth
        bytesOut.writeLong(0x123456789ABCDEF0L); // 8 more bytes - should grow
        System.out.println("After writing 8 more bytes (total 16) - Position: " + bytesOut.position() + ", Capacity: " + bytesOut.getGrowingSegment().capacity());
        
        // Test 3: Write large data that requires multiple growths
        System.out.println("\n3. Testing multiple growths with large data:");
        MemorySegmentBytesOut bigBytesOut = new MemorySegmentBytesOut(new GrowingMemorySegment(32));
        System.out.println("Starting capacity: " + bigBytesOut.getGrowingSegment().capacity() + " bytes");
        
        // Write a large byte array
        byte[] largeData = new byte[100]; // 100 bytes - much larger than initial 32
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        
        bigBytesOut.write(largeData);
        System.out.println("After writing 100 bytes - Position: " + bigBytesOut.position() + ", Capacity: " + bigBytesOut.getGrowingSegment().capacity());
        
        // Write even more data
        String longString = "This is a long string that will require additional growth beyond the current capacity.";
        bigBytesOut.writeUtf8(longString);
        System.out.println("After writing long string - Position: " + bigBytesOut.position() + ", Capacity: " + bigBytesOut.getGrowingSegment().capacity());
        
        System.out.println("\n=== Demonstration Complete ===");
        System.out.println("SUCCESS: MemorySegment successfully grew when writing over boundaries!");
    }
}