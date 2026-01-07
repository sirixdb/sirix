package io.sirix.node;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify DeweyID encoding properties for the formal proof document.
 * This test generates test vectors and verifies order preservation.
 */
class DeweyIDEncodingVerificationTest {

    @Test
    void generateTestVectors() {
        String[] testCases = {
            "1",
            "1.0",
            "1.1",
            "1.3",
            "1.17",
            "1.127",
            "1.128",
            "1.3.5",
            "1.3.5.7",
            "1.16511",
            "1.16512"
        };
        
        HexFormat hex = HexFormat.of();
        
        System.out.println("| DeweyID | divisionValues | toBytes() hex | Length |");
        System.out.println("|---------|----------------|---------------|--------|");
        
        for (String tc : testCases) {
            SirixDeweyID id = new SirixDeweyID(tc);
            byte[] bytes = id.toBytes();
            String hexStr = bytes.length == 0 ? "(empty)" : hex.formatHex(bytes);
            System.out.printf("| %s | %s | %s | %d |%n", 
                tc, 
                Arrays.toString(id.getDivisionValues()),
                hexStr,
                bytes.length);
        }
    }

    @Test
    void verifyOrderPreservation() {
        String[] ordered = {"1.3", "1.5", "1.17", "1.127", "1.128", "1.129", "1.16511", "1.16512"};
        
        System.out.println("\n--- Order Preservation Test ---");
        
        for (int i = 0; i < ordered.length - 1; i++) {
            SirixDeweyID a = new SirixDeweyID(ordered[i]);
            SirixDeweyID b = new SirixDeweyID(ordered[i + 1]);
            
            int objCmp = a.compareTo(b);
            int byteCmp = SirixDeweyID.compare(a.toBytes(), b.toBytes());
            
            boolean orderPreserved = (objCmp < 0 && byteCmp < 0);
            String status = orderPreserved ? "✓" : "✗";
            
            System.out.printf("%s < %s: obj=%d, bytes=%d %s%n", 
                ordered[i], ordered[i+1], objCmp, byteCmp, status);
            
            assertTrue(orderPreserved, 
                "Order not preserved for " + ordered[i] + " < " + ordered[i+1]);
        }
    }

    @Test
    void verifyTierBoundaries() {
        // Tier boundaries based on maxDivisionValue
        int[] tierBoundaries = {127, 16511, 2113663};
        
        System.out.println("\n--- Tier Boundary Test ---");
        
        HexFormat hex = HexFormat.of();
        
        for (int boundary : tierBoundaries) {
            // Test value at boundary and one above
            SirixDeweyID atBoundary = new SirixDeweyID("1." + boundary);
            SirixDeweyID aboveBoundary = new SirixDeweyID("1." + (boundary + 1));
            
            byte[] bytesAt = atBoundary.toBytes();
            byte[] bytesAbove = aboveBoundary.toBytes();
            
            System.out.printf("Boundary %d: len=%d, hex=%s%n", 
                boundary, bytesAt.length, hex.formatHex(bytesAt));
            System.out.printf("Above %d: len=%d, hex=%s%n", 
                boundary + 1, bytesAbove.length, hex.formatHex(bytesAbove));
            
            // Verify order preserved
            int objCmp = atBoundary.compareTo(aboveBoundary);
            int byteCmp = SirixDeweyID.compare(bytesAt, bytesAbove);
            
            assertTrue(objCmp < 0 && byteCmp < 0, 
                "Order not preserved at tier boundary " + boundary);
        }
    }

    @Test
    void verifyBijection() {
        // After fix: All values including tier boundaries should now round-trip correctly
        String[] testCases = {
            "1",
            "1.0",
            "1.1",
            "1.126",
            "1.127",   // Tier boundary - now fixed!
            "1.128",
            "1.3.5.7.9.11",
            "1.16510",
            "1.16511", // Tier boundary - now fixed!
            "1.16512",
            "1.2113662",
            "1.2113663", // Tier boundary - now fixed!
            "1.2113664"
        };
        
        System.out.println("\n--- Bijection (Round-Trip) Test ---");
        
        for (String tc : testCases) {
            SirixDeweyID original = new SirixDeweyID(tc);
            byte[] encoded = original.toBytes();
            SirixDeweyID decoded = new SirixDeweyID(encoded);
            
            boolean roundTrip = original.equals(decoded);
            String status = roundTrip ? "✓" : "✗";
            
            System.out.printf("%s -> encode -> decode: %s %s%n", 
                tc, decoded.toString(), status);
            
            assertTrue(roundTrip, "Round-trip failed for " + tc);
        }
    }
    
    @Test
    void verifyTierBoundaryFix() {
        // This test verifies that the tier boundary encoding bug has been fixed
        System.out.println("\n--- Tier Boundary Fix Verification ---");
        
        // Tier boundaries that were previously buggy
        int[] boundaries = {127, 16511, 2113663};
        
        for (int boundary : boundaries) {
            SirixDeweyID original = new SirixDeweyID("1." + boundary);
            byte[] encoded = original.toBytes();
            SirixDeweyID decoded = new SirixDeweyID(encoded);
            
            boolean correct = original.equals(decoded);
            String status = correct ? "✓ FIXED" : "✗ STILL BROKEN";
            
            System.out.printf("1.%d -> encode -> decode: %s %s%n",
                boundary, decoded.toString(), status);
            
            assertTrue(correct, "Tier boundary encoding still broken for value " + boundary);
        }
    }

    @Test
    void verifySpecialDivisionValues() {
        System.out.println("\n--- Special Division Values Test ---");
        
        // Test recordValueRoot (0)
        SirixDeweyID recordValue = new SirixDeweyID("1.0");
        System.out.printf("RecordValue (1.0): %s%n", 
            HexFormat.of().formatHex(recordValue.toBytes()));
        
        // Test attributeRoot (1) 
        SirixDeweyID attrRoot = new SirixDeweyID("1.1");
        System.out.printf("AttributeRoot (1.1): %s%n",
            HexFormat.of().formatHex(attrRoot.toBytes()));
        
        // Verify 0 < 1 in byte encoding
        assertTrue(SirixDeweyID.compare(recordValue.toBytes(), attrRoot.toBytes()) < 0,
            "recordValueRoot should be less than attributeRoot in byte encoding");
    }

    @Test
    void verifyDescendantRangeQuery() {
        System.out.println("\n--- Descendant Range Query Test ---");
        
        SirixDeweyID parent = new SirixDeweyID("1.3");
        byte[] parentBytes = parent.toBytes();
        
        String[] descendants = {"1.3.5", "1.3.7", "1.3.5.9", "1.3.17"};
        String[] nonDescendants = {"1.5", "1.2", "1.4", "1.17"};
        
        System.out.println("Parent: 1.3, bytes: " + HexFormat.of().formatHex(parentBytes));
        
        System.out.println("\nDescendants (should be in range):");
        for (String desc : descendants) {
            SirixDeweyID d = new SirixDeweyID(desc);
            byte[] dBytes = d.toBytes();
            boolean isPrefix = isPrefixOf(parentBytes, dBytes);
            System.out.printf("  %s: %s, isPrefix=%b%n", 
                desc, HexFormat.of().formatHex(dBytes), isPrefix);
            assertTrue(isPrefix, desc + " should have parent as prefix");
        }
        
        System.out.println("\nNon-descendants (should NOT be in range):");
        for (String nd : nonDescendants) {
            SirixDeweyID d = new SirixDeweyID(nd);
            byte[] dBytes = d.toBytes();
            boolean isPrefix = isPrefixOf(parentBytes, dBytes);
            System.out.printf("  %s: %s, isPrefix=%b%n", 
                nd, HexFormat.of().formatHex(dBytes), isPrefix);
        }
    }
    
    private boolean isPrefixOf(byte[] prefix, byte[] key) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (prefix[i] != key[i]) return false;
        }
        return true;
    }
}

