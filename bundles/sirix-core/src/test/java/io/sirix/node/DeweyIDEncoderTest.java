package io.sirix.node;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DeweyIDEncoder} to verify it produces identical output
 * to the original {@link SirixDeweyID#toBytes()} method.
 */
class DeweyIDEncoderTest {
    
    private static final HexFormat HEX = HexFormat.of();
    
    @Test
    @DisplayName("Encoder produces identical output to original for simple DeweyIDs")
    void testSimpleDeweyIDs() {
        String[] testCases = {
            "1",
            "1.0",
            "1.1",
            "1.3",
            "1.17",
            "1.126",  // Max Tier 0
            "1.3.5",
            "1.3.5.7",
            "1.3.5.7.9.11"
        };
        
        for (String tc : testCases) {
            verifyEncodingMatch(tc);
        }
    }
    
    @Test
    @DisplayName("Encoder handles tier boundaries correctly")
    void testTierBoundaries() {
        // Tier 0 boundary
        verifyEncodingMatch("1.126");   // Max Tier 0
        verifyEncodingMatch("1.127");   // Min Tier 1
        verifyEncodingMatch("1.128");
        
        // Tier 1 boundary
        verifyEncodingMatch("1.16510"); // Max Tier 1
        verifyEncodingMatch("1.16511"); // Min Tier 2
        verifyEncodingMatch("1.16512");
        
        // Tier 2 boundary
        verifyEncodingMatch("1.2113662"); // Max Tier 2
        verifyEncodingMatch("1.2113663"); // Min Tier 3
        verifyEncodingMatch("1.2113664");
        
        // Tier 3 boundary
        verifyEncodingMatch("1.270549118"); // Max Tier 3
        verifyEncodingMatch("1.270549119"); // Min Tier 4
        verifyEncodingMatch("1.270549120");
    }
    
    @Test
    @DisplayName("Encoder handles multi-tier DeweyIDs")
    void testMultiTierDeweyIDs() {
        // Mix of different tiers
        verifyEncodingMatch("1.0.127.16511.2113663");
        verifyEncodingMatch("1.126.127.128");
        verifyEncodingMatch("1.1.1.1.1.1.1.1"); // All Tier 0
        verifyEncodingMatch("1.1000.1000.1000"); // All Tier 1
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 10, 50, 100, 126, 127, 128, 500, 1000, 5000, 10000, 16510, 16511, 16512, 100000, 1000000, 2113662, 2113663, 2113664})
    @DisplayName("Encoder matches original for various division values")
    void testVariousDivisionValues(int value) {
        verifyEncodingMatch("1." + value);
    }
    
    @Test
    @DisplayName("Encoder handles deep hierarchies")
    void testDeepHierarchy() {
        StringBuilder sb = new StringBuilder("1");
        for (int i = 0; i < 20; i++) {
            sb.append(".").append(i * 2 + 1);
        }
        verifyEncodingMatch(sb.toString());
    }
    
    @Test
    @DisplayName("getBitsForValue returns correct values")
    void testGetBitsForValue() {
        // Tier 0: 8 bits
        assertEquals(8, DeweyIDEncoder.getBitsForValue(0));
        assertEquals(8, DeweyIDEncoder.getBitsForValue(126));
        
        // Tier 1: 16 bits
        assertEquals(16, DeweyIDEncoder.getBitsForValue(127));
        assertEquals(16, DeweyIDEncoder.getBitsForValue(16510));
        
        // Tier 2: 24 bits
        assertEquals(24, DeweyIDEncoder.getBitsForValue(16511));
        assertEquals(24, DeweyIDEncoder.getBitsForValue(2113662));
        
        // Tier 3: 32 bits
        assertEquals(32, DeweyIDEncoder.getBitsForValue(2113663));
        assertEquals(32, DeweyIDEncoder.getBitsForValue(270549118));
        
        // Tier 4: 35 bits
        assertEquals(35, DeweyIDEncoder.getBitsForValue(270549119));
        assertEquals(35, DeweyIDEncoder.getBitsForValue(Integer.MAX_VALUE));
    }
    
    @Test
    @DisplayName("getTier returns correct tier")
    void testGetTier() {
        assertEquals(0, DeweyIDEncoder.getTier(0));
        assertEquals(0, DeweyIDEncoder.getTier(126));
        assertEquals(1, DeweyIDEncoder.getTier(127));
        assertEquals(1, DeweyIDEncoder.getTier(16510));
        assertEquals(2, DeweyIDEncoder.getTier(16511));
        assertEquals(2, DeweyIDEncoder.getTier(2113662));
        assertEquals(3, DeweyIDEncoder.getTier(2113663));
        assertEquals(3, DeweyIDEncoder.getTier(270549118));
        assertEquals(4, DeweyIDEncoder.getTier(270549119));
        assertEquals(4, DeweyIDEncoder.getTier(Integer.MAX_VALUE));
    }
    
    @Test
    @DisplayName("calculateTotalBits is correct")
    void testCalculateTotalBits() {
        // Single Tier 0 division: 8 bits
        assertEquals(8, DeweyIDEncoder.calculateTotalBits(new int[]{1, 0}));
        
        // Two Tier 0 divisions: 16 bits
        assertEquals(16, DeweyIDEncoder.calculateTotalBits(new int[]{1, 0, 1}));
        
        // One Tier 1 division: 16 bits
        assertEquals(16, DeweyIDEncoder.calculateTotalBits(new int[]{1, 127}));
        
        // Mixed: Tier 0 (8) + Tier 1 (16) = 24 bits
        assertEquals(24, DeweyIDEncoder.calculateTotalBits(new int[]{1, 0, 127}));
    }
    
    @Test
    @DisplayName("Empty and root DeweyIDs produce empty arrays")
    void testEmptyEncodings() {
        assertArrayEquals(new byte[0], DeweyIDEncoder.encode(null));
        assertArrayEquals(new byte[0], DeweyIDEncoder.encode(new int[0]));
        assertArrayEquals(new byte[0], DeweyIDEncoder.encode(new int[]{1}));
    }
    
    @Test
    @DisplayName("Random DeweyIDs produce identical output")
    void testRandomDeweyIDs() {
        Random random = new Random(42); // Fixed seed for reproducibility
        
        for (int trial = 0; trial < 100; trial++) {
            int numDivisions = random.nextInt(10) + 1;
            int[] divisions = new int[numDivisions + 1];
            divisions[0] = 1;
            
            StringBuilder sb = new StringBuilder("1");
            for (int i = 1; i <= numDivisions; i++) {
                // Random value in different tier ranges
                int tier = random.nextInt(4);
                int value = switch (tier) {
                    case 0 -> random.nextInt(127);        // Tier 0
                    case 1 -> 127 + random.nextInt(16384); // Tier 1
                    case 2 -> 16511 + random.nextInt(100000); // Tier 2
                    default -> 2113663 + random.nextInt(1000000); // Tier 3
                };
                divisions[i] = value;
                sb.append(".").append(value);
            }
            
            verifyEncodingMatch(sb.toString());
        }
    }
    
    @Test
    @DisplayName("encodeToBuffer works correctly")
    void testEncodeToBuffer() {
        SirixDeweyID id = new SirixDeweyID("1.3.5.7");
        byte[] expected = id.toBytes();
        
        byte[] buffer = new byte[64];
        int length = DeweyIDEncoder.encodeToBuffer(id.getDivisionValues(), buffer, 0);
        
        assertEquals(expected.length, length);
        assertArrayEquals(expected, Arrays.copyOf(buffer, length));
    }
    
    @Test
    @DisplayName("Thread-local buffer is reusable")
    void testThreadLocalBuffer() {
        byte[] buffer1 = DeweyIDEncoder.getThreadLocalBuffer();
        byte[] buffer2 = DeweyIDEncoder.getThreadLocalBuffer();
        assertTrue(buffer1 == buffer2, "Thread-local buffer should be same instance");
        assertTrue(buffer1.length >= 64, "Buffer should be at least 64 bytes");
    }
    
    @Test
    @DisplayName("Encoder works with various gap configurations")
    void testVariousGapConfigurations() {
        // Common gap values that might be used for distanceToSibling
        int[] gapValues = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
        
        for (int gap : gapValues) {
            // Simulate a typical DeweyID sequence with this gap
            // First child: 1.(gap+1), second child: 1.(2*gap+1), etc.
            for (int childNum = 1; childNum <= 10; childNum++) {
                int divisionValue = childNum * gap + 1;
                String deweyIdStr = "1." + divisionValue;
                verifyEncodingMatch(deweyIdStr);
            }
        }
    }
    
    @Test
    @DisplayName("Encoder handles newBetween-style DeweyIDs")
    void testNewBetweenStyleIds() {
        // When inserting between siblings, IDs can have various patterns
        // Test patterns like 1.3, 1.5, 1.17, 1.33, 1.49, etc.
        String[] patterns = {
            "1.3", "1.5", "1.7", "1.9",           // Odd values
            "1.17", "1.19", "1.21", "1.23",       // gap=16 typical values
            "1.33", "1.49", "1.65", "1.81",       // gap=16 more values
            "1.2.17", "1.2.33", "1.2.49",         // Nested with gap values
            "1.17.17", "1.17.33",                  // Deeper nesting
        };
        
        for (String pattern : patterns) {
            verifyEncodingMatch(pattern);
        }
    }
    
    @Test
    @DisplayName("Encoder handles odd division values correctly")
    void testOddDivisionValues() {
        // DeweyIDs typically use odd numbers for nodes (even for internal structure)
        for (int i = 1; i <= 127; i += 2) {
            verifyEncodingMatch("1." + i);
        }
    }
    
    @Test
    @DisplayName("Encoder handles even division values correctly")
    void testEvenDivisionValues() {
        // Even values are used in internal DeweyID structure (like attribute roots)
        for (int i = 0; i <= 126; i += 2) {
            verifyEncodingMatch("1." + i);
        }
    }
    
    @Test
    @DisplayName("SirixDeweyID.setDistanceToSibling() works correctly")
    void testConfigurableSiblingDistance() {
        int originalDistance = SirixDeweyID.getDistanceToSibling();
        try {
            // Test various gap configurations
            int[] testDistances = {8, 16, 32, 64, 128, 256};
            
            for (int distance : testDistances) {
                SirixDeweyID.setDistanceToSibling(distance);
                assertEquals(distance, SirixDeweyID.getDistanceToSibling());
                
                // Create a new child ID and verify it uses the correct distance
                SirixDeweyID parent = new SirixDeweyID("1.3");
                SirixDeweyID child = parent.getNewChildID();
                
                // Child should be 1.3.(distance+1)
                int[] childDivs = child.getDivisionValues();
                assertEquals(3, childDivs.length);
                assertEquals(1, childDivs[0]);
                assertEquals(3, childDivs[1]);
                assertEquals(distance + 1, childDivs[2]);
                
                // Verify encoding still works
                byte[] encoded = child.toBytes();
                SirixDeweyID decoded = new SirixDeweyID(encoded);
                assertTrue(child.equals(decoded), "Round-trip failed for distance=" + distance);
            }
        } finally {
            // Restore original distance
            SirixDeweyID.setDistanceToSibling(originalDistance);
        }
    }
    
    @Test
    @DisplayName("SirixDeweyID.setDistanceToSibling() validates input")
    void testSiblingDistanceValidation() {
        int originalDistance = SirixDeweyID.getDistanceToSibling();
        try {
            // Should reject negative values
            assertThrows(IllegalArgumentException.class, () -> SirixDeweyID.setDistanceToSibling(-1));
            
            // Should reject zero
            assertThrows(IllegalArgumentException.class, () -> SirixDeweyID.setDistanceToSibling(0));
            
            // Should reject odd values
            assertThrows(IllegalArgumentException.class, () -> SirixDeweyID.setDistanceToSibling(15));
            assertThrows(IllegalArgumentException.class, () -> SirixDeweyID.setDistanceToSibling(33));
            
            // Should accept valid even values
            SirixDeweyID.setDistanceToSibling(2);
            assertEquals(2, SirixDeweyID.getDistanceToSibling());
            
            SirixDeweyID.setDistanceToSibling(1024);
            assertEquals(1024, SirixDeweyID.getDistanceToSibling());
        } finally {
            SirixDeweyID.setDistanceToSibling(originalDistance);
        }
    }
    
    @Test
    @DisplayName("SirixDeweyID.resetDistanceToSibling() restores default")
    void testResetSiblingDistance() {
        int originalDistance = SirixDeweyID.getDistanceToSibling();
        try {
            SirixDeweyID.setDistanceToSibling(128);
            assertEquals(128, SirixDeweyID.getDistanceToSibling());
            
            SirixDeweyID.resetDistanceToSibling();
            assertEquals(SirixDeweyID.DEFAULT_SIBLING_DISTANCE, SirixDeweyID.getDistanceToSibling());
        } finally {
            SirixDeweyID.setDistanceToSibling(originalDistance);
        }
    }
    
    @Test
    @DisplayName("Unaligned encoding works correctly")
    void testUnalignedEncoding() {
        // DeweyID with multiple divisions where bits don't align to bytes
        // 1.3 = 8 bits, 1.3.5 = 16 bits total
        // 1.3.5.7 = 24 bits total
        // All should match original
        verifyEncodingMatch("1.3");
        verifyEncodingMatch("1.3.5");
        verifyEncodingMatch("1.3.5.7");
        verifyEncodingMatch("1.3.5.7.9");
        
        // Mix Tier 0 and Tier 1 (unaligned at junction)
        verifyEncodingMatch("1.126.127"); // 8 bits + 16 bits = 24 bits
        verifyEncodingMatch("1.0.127");   // 8 bits + 16 bits = 24 bits
    }
    
    private void verifyEncodingMatch(String deweyIdStr) {
        SirixDeweyID id = new SirixDeweyID(deweyIdStr);
        byte[] original = id.toBytes();
        byte[] optimized = DeweyIDEncoder.encode(id.getDivisionValues());
        
        if (!Arrays.equals(original, optimized)) {
            System.err.printf("MISMATCH for %s%n", deweyIdStr);
            System.err.printf("  divisions: %s%n", Arrays.toString(id.getDivisionValues()));
            System.err.printf("  original:  %s (len=%d)%n", 
                original.length == 0 ? "(empty)" : HEX.formatHex(original), original.length);
            System.err.printf("  optimized: %s (len=%d)%n", 
                optimized.length == 0 ? "(empty)" : HEX.formatHex(optimized), optimized.length);
        }
        
        assertArrayEquals(original, optimized, 
            "Encoding mismatch for " + deweyIdStr + 
            ": original=" + HEX.formatHex(original) + 
            ", optimized=" + HEX.formatHex(optimized));
    }
}

