package io.sirix.io.bytepipe;

import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.utils.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.*;

/**
 * FFI-based LZ4 compression/decompression using Foreign Function API.
 * Provides zero-copy compression with MemorySegments.
 * Falls back to lz4-java if native library is unavailable.
 */
public final class FFILz4Compressor implements ByteHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(FFILz4Compressor.class);

  private static final Linker LINKER = Linker.nativeLinker();
  private static final boolean NATIVE_LZ4_AVAILABLE;

  // LZ4 native function handles
  private static final MethodHandle LZ4_COMPRESS_DEFAULT;
  private static final MethodHandle LZ4_DECOMPRESS_SAFE;
  private static final MethodHandle LZ4_COMPRESS_BOUND;

  // Fallback to lz4-java if native not available
  private static final LZ4Compressor FALLBACK = new LZ4Compressor();

  // Memory segment allocator for decompression buffers
  private static final MemorySegmentAllocator ALLOCATOR =
      OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() : LinuxMemorySegmentAllocator.getInstance();

  /**
   * ThreadLocal reusable decompression buffer to avoid per-page allocation.
   * Each thread gets its own buffer that grows as needed and is reused across page reads.
   * 
   * <p>This is safe because I/O threads in Sirix are platform threads with bounded count.
   * The buffer remains valid until the next decompress() call on the same thread,
   * which matches the usage pattern (deserialize immediately after decompress).
   * 
   * <p>Note: If virtual threads are used for I/O in the future, consider using
   * explicit buffer management or a different pooling strategy.
   */
  private static final ThreadLocal<MemorySegment> DECOMPRESSION_BUFFER = new ThreadLocal<>();
  
  /**
   * Default initial size for decompression buffer (128KB - typical page size after decompression).
   */
  private static final int DEFAULT_DECOMPRESS_BUFFER_SIZE = 128 * 1024;

  static {
    boolean available = false;
    MethodHandle compress = null;
    MethodHandle decompress = null;
    MethodHandle compressBound = null;

    try {
      // Try to load native LZ4 library
      // Try different library names (lz4, lz4.so.1, etc.)
      SymbolLookup lz4Lib = null;
      for (String libName : new String[]{"lz4", "lz4.so.1", "liblz4", "liblz4.so.1"}) {
        try {
          lz4Lib = SymbolLookup.libraryLookup(libName, Arena.global());
          LOGGER.info("Loaded LZ4 library: {}", libName);
          break;
        } catch (Exception e) {
          // Try next name
        }
      }
      
      if (lz4Lib == null) {
        throw new RuntimeException("Could not load any LZ4 library variant");
      }

      // int LZ4_compress_default(const char* src, char* dst, int srcSize, int dstCapacity)
      compress = LINKER.downcallHandle(
          lz4Lib.find("LZ4_compress_default").orElseThrow(),
          FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT)
      );

      // int LZ4_decompress_safe(const char* src, char* dst, int compressedSize, int dstCapacity)
      decompress = LINKER.downcallHandle(
          lz4Lib.find("LZ4_decompress_safe").orElseThrow(),
          FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT)
      );

      // int LZ4_compressBound(int inputSize)
      compressBound = LINKER.downcallHandle(
          lz4Lib.find("LZ4_compressBound").orElseThrow(),
          FunctionDescriptor.of(JAVA_INT, JAVA_INT)
      );

      available = true;
      LOGGER.info("Native LZ4 library loaded successfully via FFI");
    } catch (Exception e) {
      LOGGER.warn("Native LZ4 library not available, falling back to lz4-java: {}", e.getMessage());
    }

    NATIVE_LZ4_AVAILABLE = available;
    LZ4_COMPRESS_DEFAULT = compress;
    LZ4_DECOMPRESS_SAFE = decompress;
    LZ4_COMPRESS_BOUND = compressBound;
  }

  @Override
  public OutputStream serialize(OutputStream toSerialize) {
    // Delegate to fallback for stream-based API
    return FALLBACK.serialize(toSerialize);
  }

  @Override
  public InputStream deserialize(InputStream toDeserialize) {
    // Delegate to fallback for stream-based API
    return FALLBACK.deserialize(toDeserialize);
  }

  @Override
  public ByteHandler getInstance() {
    return new FFILz4Compressor();
  }

  @Override
  public boolean supportsMemorySegments() {
    return NATIVE_LZ4_AVAILABLE;
  }

  /**
   * Compress data from source MemorySegment to destination MemorySegment.
   * Zero-copy operation using FFI.
   *
   * @param source uncompressed data
   * @param destination buffer for compressed data (must be large enough)
   * @return number of compressed bytes written, or negative on error
   */
  public int compressSegment(MemorySegment source, MemorySegment destination) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    try {
      return (int) LZ4_COMPRESS_DEFAULT.invokeExact(
          source,
          destination,
          (int) source.byteSize(),
          (int) destination.byteSize()
      );
    } catch (Throwable e) {
      throw new RuntimeException("LZ4 compression failed", e);
    }
  }

  /**
   * Decompress data from source MemorySegment to destination MemorySegment.
   * Zero-copy operation using FFI.
   *
   * @param source compressed data
   * @param destination buffer for decompressed data (must be large enough)
   * @param compressedSize size of compressed data in bytes
   * @return number of decompressed bytes written, or negative on error
   */
  public int decompressSegment(MemorySegment source, MemorySegment destination, int compressedSize) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    try {
      return (int) LZ4_DECOMPRESS_SAFE.invokeExact(
          source,
          destination,
          compressedSize,
          (int) destination.byteSize()
      );
    } catch (Throwable e) {
      throw new RuntimeException("LZ4 decompression failed", e);
    }
  }

  /**
   * Calculate maximum compressed size for given input size.
   *
   * @param inputSize size of input data in bytes
   * @return maximum possible compressed size
   */
  public int compressBound(int inputSize) {
    if (!NATIVE_LZ4_AVAILABLE) {
      // Conservative estimate if native not available
      return inputSize + (inputSize / 255) + 16;
    }

    try {
      return (int) LZ4_COMPRESS_BOUND.invokeExact(inputSize);
    } catch (Throwable e) {
      throw new RuntimeException("LZ4 compressBound failed", e);
    }
  }

  /**
   * Compress data and return a new MemorySegment with compressed data.
   * Uses a confined Arena for temporary compression buffer.
   *
   * @param source uncompressed data (can be heap or native)
   * @return compressed data in a MemorySegment
   */
  @Override
  public MemorySegment compress(MemorySegment source) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    int srcSize = (int) source.byteSize();
    int maxDstSize = compressBound(srcSize);

    // Use Arena for temporary compression buffer
    try (Arena arena = Arena.ofConfined()) {
      // Allocate temporary buffer with header space
      MemorySegment tempCompressed = arena.allocate(maxDstSize + 4);

      // Write decompressed size header (for decompression)
      tempCompressed.set(JAVA_INT, 0, srcSize);

      // Compress (source must be off-heap/native memory)
      int compressedSize = compressSegment(source, tempCompressed.asSlice(4));
      if (compressedSize <= 0) {
        throw new RuntimeException("LZ4 compression failed: " + compressedSize);
      }

      // Copy to auto arena result that persists
      int totalSize = compressedSize + 4;
      MemorySegment result = Arena.ofAuto().allocate(totalSize);
      MemorySegment.copy(tempCompressed, 0, result, 0, totalSize);
      return result;
    }
  }

  /**
   * Decompress data using a ThreadLocal reusable buffer to avoid per-page allocation.
   * The buffer is grown as needed and reused across page reads on the same thread.
   *
   * @param compressed compressed data (with 4-byte header containing decompressed size)
   * @return a slice of the ThreadLocal buffer containing decompressed data
   *         (valid until next decompress call on this thread)
   */
  @Override
  public MemorySegment decompress(MemorySegment compressed) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    // Read decompressed size from header
    int decompressedSize = compressed.get(JAVA_INT, 0);
    
    // Get or grow ThreadLocal buffer
    MemorySegment buffer = DECOMPRESSION_BUFFER.get();
    if (buffer == null || buffer.byteSize() < decompressedSize) {
      int newSize = Math.max(decompressedSize, DEFAULT_DECOMPRESS_BUFFER_SIZE);
      newSize = Math.max(newSize, buffer == null ? 0 : (int) buffer.byteSize() * 2);
      buffer = Arena.ofAuto().allocate(newSize);
      DECOMPRESSION_BUFFER.set(buffer);
      LOGGER.debug("Grew decompression buffer to {} bytes for thread {}", 
                   newSize, Thread.currentThread().getName());
    }

    // Decompress into the reusable buffer
    int actualSize = decompressSegment(
        compressed.asSlice(4),
        buffer,
        (int) compressed.byteSize() - 4
    );

    if (actualSize < 0) {
      throw new RuntimeException("LZ4 decompression failed: " + actualSize);
    }

    if (actualSize != decompressedSize) {
      LOGGER.warn("Decompressed size mismatch: expected {}, got {}", decompressedSize, actualSize);
    }

    // Return a slice of the buffer (valid until next decompress call on this thread)
    return buffer.asSlice(0, actualSize);
  }
  
  /**
   * Clear the ThreadLocal decompression buffer for the current thread.
   * Call this when shutting down or when memory needs to be reclaimed.
   */
  public static void clearDecompressionBuffer() {
    DECOMPRESSION_BUFFER.remove();
  }

  /**
   * Check if native LZ4 is available.
   */
  public static boolean isNativeAvailable() {
    return NATIVE_LZ4_AVAILABLE;
  }
}

