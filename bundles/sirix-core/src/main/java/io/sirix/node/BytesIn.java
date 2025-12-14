package io.sirix.node;

import java.math.BigInteger;

/**
 * Custom BytesIn interface for reading data from various sources.
 * This interface replaces the Chronicle Bytes dependency and provides
 * a clean abstraction for reading serialized data.
 * 
 * @param <T> the underlying data source type
 */
public interface BytesIn<T> {
    
    /**
     * Read an integer value.
     * @return the integer value
     */
    int readInt();
    
    /**
     * Read a long value.
     * @return the long value
     */
    long readLong();
    
    /**
     * Read a byte value.
     * @return the byte value
     */
    byte readByte();
    
    /**
     * Read a boolean value.
     * @return the boolean value
     */
    boolean readBoolean();
    
    /**
     * Read a double value.
     * @return the double value
     */
    double readDouble();
    
    /**
     * Read a float value.
     * @return the float value
     */
    float readFloat();
    
    /**
     * Read a short value.
     * @return the short value
     */
    short readShort();
    
    /**
     * Read a BigInteger value.
     * @return the BigInteger value
     */
    BigInteger readBigInteger();
    
    /**
     * Read a UTF-8 string.
     * @return the string value, or null if the string is null
     */
    String readUtf8();
    
    /**
     * Read a variable-length long value using stop-bit encoding.
     * @return the long value
     */
    long readStopBit();
    
    /**
     * Read a byte array.
     * @param bytes the byte array to fill
     */
    void read(byte[] bytes);
    
    /**
     * Read a portion of a byte array.
     * @param bytes the byte array to fill
     * @param offset the offset in the array
     * @param length the number of bytes to read
     */
    void read(byte[] bytes, int offset, int length);
    
    /**
     * Get the current read position.
     * @return the current position
     */
    long position();
    
    /**
     * Set the current read position.
     * @param newPosition the new position
     */
    void position(long newPosition);
    
    /**
     * Skip forward by the specified number of bytes.
     * This is equivalent to {@code position(position() + bytes)} but may be more efficient.
     * 
     * @param bytes number of bytes to skip (must be non-negative)
     * @throws IllegalArgumentException if bytes is negative
     * @throws IndexOutOfBoundsException if skip would exceed the data bounds
     */
    void skip(long bytes);
    
    /**
     * Get the remaining bytes from current position.
     * @return the number of remaining bytes
     */
    long remaining();
    
    /**
     * Get the underlying data source.
     * @return the data source
     */
    T getSource();

    /**
     * Get the underlying data destination.
     * @return the data destination
     */
    T getUnderlying();
    
    /**
     * Get an InputStream view of this BytesIn.
     * @return an InputStream that reads from this BytesIn
     */
    java.io.InputStream inputStream();
    
    /**
     * Convert remaining data to byte array.
     * @return byte array containing remaining data
     */
    byte[] toByteArray();
}