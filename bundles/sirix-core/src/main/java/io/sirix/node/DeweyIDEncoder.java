package io.sirix.node;

/**
 * High-performance DeweyID encoder optimized for minimal branch mispredictions
 * and direct byte manipulation.
 * 
 * <p>This encoder replaces the bit-by-bit encoding in {@link SirixDeweyID#toBytes()}
 * with lookup tables and direct byte writes for the common tiers (0-3).
 * 
 * <h2>Tier Encoding</h2>
 * <pre>
 * Tier 0: prefix "0"    + 7-bit suffix  =  8 bits (1 byte)  - values 0-126
 * Tier 1: prefix "10"   + 14-bit suffix = 16 bits (2 bytes) - values 127-16510
 * Tier 2: prefix "110"  + 21-bit suffix = 24 bits (3 bytes) - values 16511-2113662
 * Tier 3: prefix "1110" + 28-bit suffix = 32 bits (4 bytes) - values 2113663-270546942
 * Tier 4: prefix "1111" + 31-bit suffix = 35 bits           - values 270546943+
 * </pre>
 * 
 * <h2>Performance</h2>
 * <ul>
 *   <li>Tier 0-3: Single table lookup + direct byte writes (zero bit manipulation)</li>
 *   <li>Thread-local buffer eliminates allocation on hot path</li>
 *   <li>Branchless tier selection via binary search pattern</li>
 * </ul>
 * 
 * @author SirixDB Team
 * @since 0.10.0
 */
public final class DeweyIDEncoder {
    
    // Tier boundaries (exclusive upper bounds after fix)
    // Calculated as: maxDivisionValue[i] = (1 << divisionLengthArray[i]) + maxDivisionValue[i-1]
    // where divisionLengthArray = {7, 14, 21, 28, 31}
    private static final int TIER_0_MAX = 127;        // Values [0, 127) use Tier 0
    private static final int TIER_1_MAX = 16511;      // Values [127, 16511) use Tier 1  (127 + 16384)
    private static final int TIER_2_MAX = 2113663;    // Values [16511, 2113663) use Tier 2  (16511 + 2097152)
    private static final int TIER_3_MAX = 270549119;  // Values [2113663, 270549119) use Tier 3  (2113663 + 268435456)
    
    // Bits per tier (total = prefix + suffix)
    private static final int TIER_0_BITS = 8;   // 1 + 7
    private static final int TIER_1_BITS = 16;  // 2 + 14
    private static final int TIER_2_BITS = 24;  // 3 + 21
    private static final int TIER_3_BITS = 32;  // 4 + 28
    private static final int TIER_4_BITS = 35;  // 4 + 31
    
    // Pre-computed lookup table for Tier 0 (values 0-126)
    // Each byte encodes: prefix "0" (bit 7) + suffix bits 6-0 = (value + 1)
    private static final byte[] TIER_0_TABLE = new byte[127];
    
    static {
        for (int v = 0; v < 127; v++) {
            // Prefix "0" is bit 7 = 0, suffix = v + 1 in bits 6-0
            TIER_0_TABLE[v] = (byte) (v + 1);
        }
    }
    
    // Thread-local buffer for zero-allocation encoding
    // Max size: 35 bits * 8 divisions = 280 bits = 35 bytes (very conservative)
    private static final ThreadLocal<byte[]> ENCODE_BUFFER = 
        ThreadLocal.withInitial(() -> new byte[64]);
    
    private DeweyIDEncoder() {
        // Utility class
    }
    
    /**
     * Encodes division values to bytes using optimized direct byte writes.
     * 
     * <p>This method is functionally equivalent to {@link SirixDeweyID#toBytes()}
     * but uses lookup tables and direct byte manipulation for performance.
     * 
     * @param divisionValues the division values (first element is always 1 and skipped)
     * @return encoded byte array
     * @throws IllegalArgumentException if any division value is negative
     */
    public static byte[] encode(int[] divisionValues) {
        if (divisionValues == null || divisionValues.length <= 1) {
            return new byte[0];
        }
        
        // Calculate total bits needed
        int totalBits = 0;
        for (int i = 1; i < divisionValues.length; i++) {
            totalBits += getBitsForValue(divisionValues[i]);
        }
        
        // Allocate result array
        int byteLength = (totalBits + 7) >>> 3; // Ceiling division by 8
        byte[] result = new byte[byteLength];
        
        // Encode each division
        int bitIndex = 0;
        for (int i = 1; i < divisionValues.length; i++) {
            bitIndex = encodeDivision(divisionValues[i], result, bitIndex);
        }
        
        return result;
    }
    
    /**
     * Encodes division values into the provided buffer.
     * 
     * @param divisionValues the division values
     * @param buffer output buffer (must be large enough)
     * @param bufferOffset starting offset in buffer
     * @return number of bytes written
     */
    public static int encodeToBuffer(int[] divisionValues, byte[] buffer, int bufferOffset) {
        if (divisionValues == null || divisionValues.length <= 1) {
            return 0;
        }
        
        int bitIndex = bufferOffset * 8;
        for (int i = 1; i < divisionValues.length; i++) {
            bitIndex = encodeDivision(divisionValues[i], buffer, bitIndex);
        }
        
        // Return number of bytes used
        int endByte = (bitIndex + 7) >>> 3;
        return endByte - bufferOffset;
    }
    
    /**
     * Returns the number of bits needed to encode a single division value.
     * Uses branchless comparison pattern for better branch prediction.
     */
    public static int getBitsForValue(int value) {
        // Binary search pattern reduces branches from 5 to ~2-3
        if (value < TIER_1_MAX) {
            return value < TIER_0_MAX ? TIER_0_BITS : TIER_1_BITS;
        } else {
            if (value < TIER_3_MAX) {
                return value < TIER_2_MAX ? TIER_2_BITS : TIER_3_BITS;
            } else {
                return TIER_4_BITS;
            }
        }
    }
    
    /**
     * Encodes a single division value starting at the given bit index.
     * Returns the bit index after encoding.
     */
    private static int encodeDivision(int value, byte[] buffer, int bitIndex) {
        if (value < 0) {
            throw new IllegalArgumentException("Division value cannot be negative: " + value);
        }
        
        if (value < TIER_0_MAX) {
            return encodeTier0(value, buffer, bitIndex);
        } else if (value < TIER_1_MAX) {
            return encodeTier1(value, buffer, bitIndex);
        } else if (value < TIER_2_MAX) {
            return encodeTier2(value, buffer, bitIndex);
        } else if (value < TIER_3_MAX) {
            return encodeTier3(value, buffer, bitIndex);
        } else {
            return encodeTier4(value, buffer, bitIndex);
        }
    }
    
    /**
     * Tier 0: 1-bit prefix "0" + 7-bit suffix = 8 bits total
     * Byte layout: [0|suffix(7 bits)] where suffix = value + 1
     */
    private static int encodeTier0(int value, byte[] buffer, int bitIndex) {
        byte encoded = TIER_0_TABLE[value];
        
        int bytePos = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        
        if (bitOffset == 0) {
            // Aligned case: direct write
            buffer[bytePos] = encoded;
        } else {
            // Unaligned: split across two bytes
            buffer[bytePos] |= (encoded & 0xFF) >>> bitOffset;
            buffer[bytePos + 1] |= (encoded << (8 - bitOffset));
        }
        
        return bitIndex + TIER_0_BITS;
    }
    
    /**
     * Tier 1: 2-bit prefix "10" + 14-bit suffix = 16 bits total
     * Prefix "10" = 0b10xxxxxx xxxxxxxx
     * Suffix = value - 127 (0 to 16383)
     */
    private static int encodeTier1(int value, byte[] buffer, int bitIndex) {
        int suffix = value - TIER_0_MAX;  // Range [0, 16383]
        // Encoding: 10 + 14-bit suffix
        // Bit layout: [10][suffix_high(6 bits)][suffix_low(8 bits)]
        int encoded = 0x8000 | suffix;  // 0b10_00000000000000 | suffix
        
        int bytePos = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        
        if (bitOffset == 0) {
            buffer[bytePos] = (byte) (encoded >>> 8);
            buffer[bytePos + 1] = (byte) encoded;
        } else {
            // Shift the 16-bit value and write across 3 bytes
            int shifted = encoded << (8 - bitOffset);
            buffer[bytePos] |= (shifted >>> 16) & 0xFF;
            buffer[bytePos + 1] |= (shifted >>> 8) & 0xFF;
            buffer[bytePos + 2] |= shifted & 0xFF;
        }
        
        return bitIndex + TIER_1_BITS;
    }
    
    /**
     * Tier 2: 3-bit prefix "110" + 21-bit suffix = 24 bits total
     * Suffix = value - 16511 (0 to 2097151)
     */
    private static int encodeTier2(int value, byte[] buffer, int bitIndex) {
        int suffix = value - TIER_1_MAX;  // Range [0, 2097152]
        // Encoding: 110 + 21-bit suffix
        // Bit layout: [110][suffix(21 bits)] = 24 bits = 3 bytes
        int encoded = 0xC00000 | suffix;  // 0b110_000000000000000000000 | suffix
        
        int bytePos = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        
        if (bitOffset == 0) {
            buffer[bytePos] = (byte) (encoded >>> 16);
            buffer[bytePos + 1] = (byte) (encoded >>> 8);
            buffer[bytePos + 2] = (byte) encoded;
        } else {
            // Shift and write across 4 bytes
            long shifted = ((long) encoded) << (8 - bitOffset);
            buffer[bytePos] |= (shifted >>> 24) & 0xFF;
            buffer[bytePos + 1] |= (shifted >>> 16) & 0xFF;
            buffer[bytePos + 2] |= (shifted >>> 8) & 0xFF;
            buffer[bytePos + 3] |= shifted & 0xFF;
        }
        
        return bitIndex + TIER_2_BITS;
    }
    
    /**
     * Tier 3: 4-bit prefix "1110" + 28-bit suffix = 32 bits total
     * Suffix = value - 2113663 (0 to 268435455)
     */
    private static int encodeTier3(int value, byte[] buffer, int bitIndex) {
        int suffix = value - TIER_2_MAX;  // Range [0, 268433279]
        // Encoding: 1110 + 28-bit suffix
        // Bit layout: [1110][suffix(28 bits)] = 32 bits = 4 bytes
        int encoded = 0xE0000000 | suffix;  // 0b1110_0000000000000000000000000000 | suffix
        
        int bytePos = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        
        if (bitOffset == 0) {
            buffer[bytePos] = (byte) (encoded >>> 24);
            buffer[bytePos + 1] = (byte) (encoded >>> 16);
            buffer[bytePos + 2] = (byte) (encoded >>> 8);
            buffer[bytePos + 3] = (byte) encoded;
        } else {
            // Shift and write across 5 bytes
            long shifted = (((long) encoded) & 0xFFFFFFFFL) << (8 - bitOffset);
            buffer[bytePos] |= (shifted >>> 32) & 0xFF;
            buffer[bytePos + 1] |= (shifted >>> 24) & 0xFF;
            buffer[bytePos + 2] |= (shifted >>> 16) & 0xFF;
            buffer[bytePos + 3] |= (shifted >>> 8) & 0xFF;
            buffer[bytePos + 4] |= shifted & 0xFF;
        }
        
        return bitIndex + TIER_3_BITS;
    }
    
    /**
     * Tier 4: 4-bit prefix "1111" + 31-bit suffix = 35 bits total
     * Suffix = value - 270549119 (0 to Integer.MAX_VALUE - 270549119)
     * 
     * This tier is NOT byte-aligned (35 bits), so we need bit manipulation.
     */
    private static int encodeTier4(int value, byte[] buffer, int bitIndex) {
        int suffix = value - TIER_3_MAX;  // Range [0, Integer.MAX_VALUE - 270549119]
        // Encoding: 1111 + 31-bit suffix = 35 bits
        // We need to write 35 bits, which spans 5 bytes
        
        // Construct the 35-bit value (using long for ease)
        // Bit layout: [1111][31-bit suffix]
        // Prefix "1111" at bits 34-31, suffix at bits 30-0
        long encoded = (0xFL << 31) | (suffix & 0x7FFFFFFFL);
        
        int bytePos = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        
        // Write 35 bits starting at bitOffset
        // This always spans 5 bytes when aligned, 6 when unaligned
        if (bitOffset == 0) {
            // Bits 34-27 (8 bits)
            buffer[bytePos] = (byte) (encoded >>> 27);
            // Bits 26-19 (8 bits)
            buffer[bytePos + 1] = (byte) (encoded >>> 19);
            // Bits 18-11 (8 bits)
            buffer[bytePos + 2] = (byte) (encoded >>> 11);
            // Bits 10-3 (8 bits)
            buffer[bytePos + 3] = (byte) (encoded >>> 3);
            // Bits 2-0 (3 bits) in the top 3 bits of byte 4
            buffer[bytePos + 4] = (byte) (encoded << 5);
        } else {
            // Unaligned case: use bit-by-bit writing for simplicity
            // (Tier 4 is rare in practice - values > 270 million)
            writeBits(buffer, bitIndex, encoded, 35);
        }
        
        return bitIndex + TIER_4_BITS;
    }
    
    /**
     * Writes a specified number of bits to the buffer starting at the given bit index.
     * Used for unaligned Tier 4 encoding.
     */
    private static void writeBits(byte[] buffer, int bitIndex, long value, int numBits) {
        for (int i = numBits - 1; i >= 0; i--) {
            if ((value & (1L << i)) != 0) {
                int bytePos = bitIndex >>> 3;
                int bitPos = 7 - (bitIndex & 7);
                buffer[bytePos] |= (1 << bitPos);
            }
            bitIndex++;
        }
    }
    
    /**
     * Returns the thread-local buffer for zero-allocation encoding.
     * Caller must copy the result if retention is needed.
     */
    public static byte[] getThreadLocalBuffer() {
        return ENCODE_BUFFER.get();
    }
    
    /**
     * Calculates the total number of bits needed to encode the given division values.
     */
    public static int calculateTotalBits(int[] divisionValues) {
        if (divisionValues == null || divisionValues.length <= 1) {
            return 0;
        }
        
        int totalBits = 0;
        for (int i = 1; i < divisionValues.length; i++) {
            totalBits += getBitsForValue(divisionValues[i]);
        }
        return totalBits;
    }
    
    /**
     * Returns the tier (0-4) for a given division value.
     */
    public static int getTier(int value) {
        if (value < TIER_0_MAX) return 0;
        if (value < TIER_1_MAX) return 1;
        if (value < TIER_2_MAX) return 2;
        if (value < TIER_3_MAX) return 3;
        return 4;
    }
}

