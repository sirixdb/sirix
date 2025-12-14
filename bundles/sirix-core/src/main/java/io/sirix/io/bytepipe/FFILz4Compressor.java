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
   * Default initial size for decompression buffer (128KB - typical page size after decompression).
   */
  private static final int DEFAULT_DECOMPRESS_BUFFER_SIZE = 128 * 1024;
  
  /**
   * Bounded pool of decompression buffers - Loom-friendly.
   * 
   * <p>Unlike ThreadLocal which creates one buffer per thread (memory explosion with
   * millions of virtual threads), this pool has a fixed size of 2×CPU cores.
   * 
   * <p>When all buffers are in use, a new one is allocated (not pooled). This ensures
   * progress under high concurrency while keeping memory bounded for typical loads.
   */
  private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
  private static final java.util.concurrent.ArrayBlockingQueue<MemorySegment> BUFFER_POOL = 
      new java.util.concurrent.ArrayBlockingQueue<>(POOL_SIZE);

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
   * <p>Handles both heap and native MemorySegments. Heap segments are copied
   * to native memory for FFI calls.
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
      // FFI requires native memory - copy heap segments to native
      MemorySegment nativeSource;
      if (source.isNative()) {
        nativeSource = source;
      } else {
        // Copy heap segment to native memory for FFI call
        nativeSource = arena.allocate(srcSize);
        MemorySegment.copy(source, 0, nativeSource, 0, srcSize);
      }
      
      // Allocate temporary buffer with header space
      MemorySegment tempCompressed = arena.allocate(maxDstSize + 4);

      // Write decompressed size header (for decompression)
      tempCompressed.set(JAVA_INT, 0, srcSize);

      // Compress (source is now guaranteed native memory)
      int compressedSize = compressSegment(nativeSource, tempCompressed.asSlice(4));
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
   * Decompress data using the scoped pool pattern.
   * 
   * @deprecated Use {@link #decompressScoped(MemorySegment)} instead for Loom compatibility.
   *             This method allocates a new buffer that must be managed by the caller.
   */
  @Override
  @Deprecated(forRemoval = true)
  public MemorySegment decompress(MemorySegment compressed) {
    // For backwards compatibility, allocate a fresh buffer (caller manages lifecycle)
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    int decompressedSize = compressed.get(JAVA_INT_UNALIGNED, 0);
    
    // Use confined arena for temporary native copy if source is heap
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment nativeCompressed;
      if (compressed.isNative()) {
        nativeCompressed = compressed;
      } else {
        // Copy heap segment to native memory for FFI call
        nativeCompressed = arena.allocate(compressed.byteSize());
        MemorySegment.copy(compressed, 0, nativeCompressed, 0, compressed.byteSize());
      }
      
      MemorySegment buffer = Arena.ofAuto().allocate(decompressedSize);

      int actualSize = decompressSegment(
          nativeCompressed.asSlice(4),
          buffer,
          (int) nativeCompressed.byteSize() - 4
      );

      if (actualSize < 0) {
        throw new RuntimeException("LZ4 decompression failed: " + actualSize);
      }

      return buffer.asSlice(0, actualSize);
    }
  }

  /**
   * Decompress data using a pooled buffer with explicit lifecycle (Loom-friendly).
   * 
   * <p>Memory is bounded by pool size (2×CPU cores), not thread count.
   * When pool is exhausted, a fresh buffer is allocated (not pooled).
   * 
   * <p>The returned result MUST be closed after use:
   * <pre>{@code
   * try (var result = compressor.decompressScoped(compressed)) {
   *     Page page = deserialize(result.segment());
   * } // Buffer returned to pool
   * }</pre>
   *
   * @param compressed compressed data (with 4-byte header containing decompressed size)
   * @return scoped result; must be closed to return buffer to pool
   */
  @Override
  public DecompressionResult decompressScoped(MemorySegment compressed) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    int decompressedSize = compressed.get(JAVA_INT_UNALIGNED, 0);
    
    // Try to get a buffer from pool (non-blocking)
    MemorySegment buffer = BUFFER_POOL.poll();
    boolean fromPool = (buffer != null);
    
    // If pool empty or buffer too small, allocate fresh
    if (buffer == null || buffer.byteSize() < decompressedSize) {
      int newSize = Math.max(decompressedSize, DEFAULT_DECOMPRESS_BUFFER_SIZE);
      if (buffer != null) {
        // Return undersized buffer to pool, allocate new
        BUFFER_POOL.offer(buffer);
        newSize = Math.max(newSize, (int) buffer.byteSize() * 2);
      }
      buffer = Arena.ofAuto().allocate(newSize);
      fromPool = false;
      LOGGER.debug("Allocated decompression buffer of {} bytes (pool size: {})", 
                   newSize, BUFFER_POOL.size());
    }

    // FFI requires native memory - handle heap segments
    MemorySegment nativeCompressed;
    Arena tempArena = null;
    if (compressed.isNative()) {
      nativeCompressed = compressed;
    } else {
      // Copy heap segment to native memory for FFI call
      tempArena = Arena.ofConfined();
      nativeCompressed = tempArena.allocate(compressed.byteSize());
      MemorySegment.copy(compressed, 0, nativeCompressed, 0, compressed.byteSize());
    }
    
    try {
      // Decompress
      int actualSize = decompressSegment(
          nativeCompressed.asSlice(4),
          buffer,
          (int) nativeCompressed.byteSize() - 4
      );

      if (actualSize < 0) {
        // Return buffer to pool on error
        BUFFER_POOL.offer(buffer);
        throw new RuntimeException("LZ4 decompression failed: " + actualSize);
      }

      if (actualSize != decompressedSize) {
        LOGGER.warn("Decompressed size mismatch: expected {}, got {}", decompressedSize, actualSize);
      }

      // Create result with releaser that returns buffer to pool
      final MemorySegment poolBuffer = buffer;
      final boolean shouldReturn = fromPool || BUFFER_POOL.size() < POOL_SIZE;
      
      return new DecompressionResult(
          buffer.asSlice(0, actualSize),
          shouldReturn ? () -> BUFFER_POOL.offer(poolBuffer) : null
      );
    } finally {
      if (tempArena != null) {
        tempArena.close();
      }
    }
  }
  
  /**
   * Clear all buffers from the pool.
   * Call this when shutting down or when memory needs to be reclaimed.
   */
  public static void clearPool() {
    BUFFER_POOL.clear();
  }
  
  /**
   * Get current pool statistics for monitoring.
   * 
   * @return number of buffers currently in pool
   */
  public static int getPoolSize() {
    return BUFFER_POOL.size();
  }

  /**
   * Check if native LZ4 is available.
   */
  public static boolean isNativeAvailable() {
    return NATIVE_LZ4_AVAILABLE;
  }
}

