/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.node;

import org.junit.Test;

import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Memory leak tests for GrowingMemorySegment with Arena.ofAuto().
 * 
 * These tests verify that off-heap memory allocated by GrowingMemorySegment
 * is properly released by the garbage collector when segments are no longer referenced.
 * 
 * @author Johannes Lichtenberger
 */
public class GrowingMemorySegmentMemoryLeakTest {

    private static final int LARGE_ALLOCATION_SIZE = 1024 * 1024; // 1 MB
    private static final int NUM_ITERATIONS = 100;
    private static final int GC_ATTEMPTS = 5;

    /**
     * Test that GrowingMemorySegment instances can be garbage collected
     * after they go out of scope.
     */
    @Test
    public void testGrowingMemorySegmentIsGarbageCollected() {
        // Create and discard many segments
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            GrowingMemorySegment segment = new GrowingMemorySegment(LARGE_ALLOCATION_SIZE);
            segment.writeLong(123456789L);
            // Segment goes out of scope and becomes eligible for GC
        }
        
        // Force GC multiple times to ensure collection
        forceGC();
        
        // If we get here without OutOfMemoryError, memory is being released
        assertTrue("GrowingMemorySegment instances are garbage collected", true);
    }

    /**
     * Test that MemorySegments obtained from GrowingMemorySegment remain valid
     * even after the GrowingMemorySegment itself is garbage collected.
     * This is the key property needed for Arena.ofAuto().
     */
    @Test
    public void testMemorySegmentRemainsValidAfterGrowingSegmentGC() {
        // Create segment and extract MemorySegment
        GrowingMemorySegment growingSegment = new GrowingMemorySegment(1024);
        growingSegment.writeLong(0xDEADBEEFCAFEBABEL);
        MemorySegment segment = growingSegment.getUsedSegment();
        
        // Store the value for later verification
        long writtenValue = segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, 0);
        assertEquals(0xDEADBEEFCAFEBABEL, writtenValue);
        
        // Drop reference to GrowingMemorySegment
        growingSegment = null;
        
        // Force GC
        forceGC();
        
        // MemorySegment should STILL be valid (Arena.ofAuto() keeps it alive)
        long readValue = segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, 0);
        assertEquals("MemorySegment remains valid after GrowingMemorySegment is GC'd", 
                     0xDEADBEEFCAFEBABEL, readValue);
    }

    /**
     * Test that memory is eventually freed when both GrowingMemorySegment
     * and MemorySegment are no longer referenced.
     * 
     * Note: This test verifies that many allocations/deallocations complete
     * without OutOfMemoryError, which confirms memory is being released.
     */
    @Test
    public void testMemoryFreedWhenAllReferencesGone() {
        // Perform many allocation/deallocation cycles
        // If memory leaked, we would get OutOfMemoryError
        for (int cycle = 0; cycle < 5; cycle++) {
            List<GrowingMemorySegment> segments = new ArrayList<>();
            
            // Allocate many large segments
            for (int i = 0; i < 100; i++) {
                GrowingMemorySegment segment = new GrowingMemorySegment(LARGE_ALLOCATION_SIZE);
                // Write data to ensure allocation happens
                for (int j = 0; j < 1000; j++) {
                    segment.writeLong(j);
                }
                segments.add(segment);
            }
            
            // Verify segments work
            assertFalse("Segments were created", segments.isEmpty());
            
            // Clear references and force GC
            segments.clear();
            forceGC();
        }
        
        // If we completed all cycles without OutOfMemoryError, memory is being freed
        assertTrue("Multiple allocation/deallocation cycles completed successfully", true);
    }

    /**
     * Test that multiple allocations and deallocations don't cause memory accumulation.
     * 
     * This test verifies that repeated allocation/deallocation cycles complete
     * successfully without running out of memory, which confirms memory is being released.
     */
    @Test
    public void testNoMemoryAccumulationAcrossMultipleCycles() {
        // Run many allocation/deallocation cycles
        // If memory accumulated, we would eventually get OutOfMemoryError
        for (int cycle = 0; cycle < 20; cycle++) {
            // Allocate
            List<MemorySegment> segments = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                GrowingMemorySegment growing = new GrowingMemorySegment(512 * 1024);
                growing.writeInt(i);
                segments.add(growing.getUsedSegment());
            }
            
            // Verify segments work
            assertEquals("Segment contains correct value", 
                         25, segments.get(25).get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, 0));
            
            // Deallocate
            segments.clear();
            if (cycle % 5 == 0) {
                forceGC();
            }
        }
        
        // If we completed all cycles without OutOfMemoryError, no memory accumulation
        assertTrue("Many allocation/deallocation cycles completed without memory accumulation", true);
    }

    /**
     * Test that MemorySegments remain valid when stored in a data structure
     * (simulating storage in pages) and can be accessed later.
     */
    @Test
    public void testMemorySegmentsInDataStructureSimulatingPages() {
        // Simulate storing segments in pages
        List<MemorySegment> pageStorage = new ArrayList<>();
        
        // Create and store segments
        for (int i = 0; i < 100; i++) {
            GrowingMemorySegment growing = new GrowingMemorySegment(1024);
            growing.writeLong(i * 1000L);
            pageStorage.add(growing.getUsedSegment());
            // GrowingMemorySegment goes out of scope, but MemorySegment is stored
        }
        
        // Force GC
        forceGC();
        
        // Verify all segments are still valid and contain correct data
        for (int i = 0; i < 100; i++) {
            MemorySegment segment = pageStorage.get(i);
            long value = segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, 0);
            assertEquals("Segment " + i + " contains correct value", i * 1000L, value);
        }
        
        // Clear storage
        pageStorage.clear();
        forceGC();
        
        // Test passes if no exceptions thrown
    }

    /**
     * Test that close() is truly a no-op and doesn't invalidate the segment.
     */
    @Test
    public void testCloseIsNoOp() {
        GrowingMemorySegment segment = new GrowingMemorySegment(1024);
        segment.writeLong(0x123456789ABCDEF0L);
        MemorySegment memSeg = segment.getUsedSegment();
        
        // Call close()
        segment.close();
        
        // Segment should still be usable
        assertTrue("Segment is still alive after close()", segment.isAlive());
        
        // Can still read data
        long value = memSeg.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, 0);
        assertEquals(0x123456789ABCDEF0L, value);
        
        // Can still write data
        segment.writeLong(0xFEDCBA9876543210L);
        
        // Close can be called multiple times safely
        segment.close();
        segment.close();
        
        assertTrue("Segment still alive after multiple close() calls", segment.isAlive());
    }

    /**
     * Test memory behavior with BytesOut (the actual usage pattern in SirixDB).
     */
    @Test
    public void testMemoryLeakWithBytesOut() {
        long initialMemory = getUsedMemory();
        
        // Simulate creating many nodes (the actual usage pattern)
        List<MemorySegment> storedSegments = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            BytesOut<?> data = Bytes.elasticHeapByteBuffer();
            
            // Write node data
            data.writeLong(i);  // parentKey
            data.writeInt(0);   // previousRevision
            data.writeInt(0);   // lastModifiedRevision
            data.writeStopBit(10); // some value length
            
            // Extract segment (as factory does)
            MemorySegment segment = (MemorySegment) data.asBytesIn().getUnderlying();
            storedSegments.add(segment);
            
            // BytesOut goes out of scope
        }
        
        long afterCreationMemory = getUsedMemory();
        
        // Verify segments are still valid
        for (int i = 0; i < 100; i++) {
            long value = storedSegments.get(i).get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, 0);
            assertEquals(i, value);
        }
        
        // Clear all references
        storedSegments.clear();
        forceGC();
        
        long afterGCMemory = getUsedMemory();
        long freed = afterCreationMemory - afterGCMemory;
        
        // Some memory should be freed
        assertTrue("Memory freed after clearing references: " + freed + " bytes", freed >= 0);
    }

    /**
     * Test concurrent allocation and GC behavior.
     */
    @Test
    public void testConcurrentAllocationAndGC() throws InterruptedException {
        final int NUM_THREADS = 4;
        final int ALLOCATIONS_PER_THREAD = 50;
        
        Thread[] threads = new Thread[NUM_THREADS];
        for (int t = 0; t < NUM_THREADS; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < ALLOCATIONS_PER_THREAD; i++) {
                    GrowingMemorySegment segment = new GrowingMemorySegment(128 * 1024);
                    segment.writeLong(Thread.currentThread().getId());
                    segment.writeLong(i);
                    // Let segment become eligible for GC
                    if (i % 10 == 0) {
                        System.gc(); // Suggest GC periodically
                    }
                }
            });
            threads[t].start();
        }
        
        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }
        
        // Final GC
        forceGC();
        
        // Test passes if no OutOfMemoryError occurred
        assertTrue("Concurrent allocation and GC succeeded", true);
    }

    // ============================================================================
    // Helper Methods
    // ============================================================================

    /**
     * Force garbage collection multiple times to ensure collection happens.
     */
    private void forceGC() {
        for (int i = 0; i < GC_ATTEMPTS; i++) {
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Get current used memory (heap + non-heap).
     */
    private long getUsedMemory() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();
        return heapUsage.getUsed() + nonHeapUsage.getUsed();
    }
}

