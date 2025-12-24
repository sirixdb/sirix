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

  /**
   * Compression mode for LZ4.
   * <ul>
   *   <li>{@link #FAST} - Optimized for write-heavy workloads (bulk imports, shredding).
   *       Uses LZ4_compress_fast which is ~10x faster than HC with ~95% of the compression ratio.</li>
   *   <li>{@link #HIGH_COMPRESSION} - Optimized for read-heavy or storage-constrained workloads.
   *       Uses LZ4_compress_HC which provides better compression but is 10-20x slower.
   *       Decompression speed is identical for both modes.</li>
   * </ul>
   */
  public enum CompressionMode {
    /**
     * Fast compression mode - optimized for write throughput.
     * Best for: bulk imports, shredding, write-heavy workloads.
     */
    FAST,
    
    /**
     * High compression mode - optimized for storage efficiency.
     * Best for: read-heavy workloads, storage-constrained environments.
     * Note: Decompression is equally fast for both modes.
     */
    HIGH_COMPRESSION
  }

  private static final Linker LINKER = Linker.nativeLinker();
  private static final boolean NATIVE_LZ4_AVAILABLE;

  // LZ4 native function handles
  private static final MethodHandle LZ4_COMPRESS_DEFAULT;
  private static final MethodHandle LZ4_COMPRESS_FAST;
  private static final MethodHandle LZ4_COMPRESS_HC;
  private static final MethodHandle LZ4_DECOMPRESS_SAFE;
  private static final MethodHandle LZ4_DECOMPRESS_FAST;
  private static final MethodHandle LZ4_COMPRESS_BOUND;
  
  /**
   * Whether to use LZ4_decompress_fast instead of LZ4_decompress_safe.
   * 
   * <p><b>NOTE:</b> In LZ4 1.9.x, LZ4_decompress_fast was deprecated and is actually
   * SLOWER than LZ4_decompress_safe in benchmarks. This flag is kept for testing only.
   * 
   * <p>Set via system property: -Dsirix.lz4.fast.decompress=true
   */
  private static final boolean USE_FAST_DECOMPRESS = 
      Boolean.getBoolean("sirix.lz4.fast.decompress");

  /**
   * LZ4HC compression level (1-12, where 9 is the default providing best ratio/speed tradeoff).
   * Higher values provide better compression but are slower.
   */
  private static final int LZ4HC_CLEVEL_DEFAULT = 9;

  /**
   * LZ4 fast mode acceleration (1=default, higher=faster but worse ratio).
   * Value of 2 provides ~30% faster compression with ~3% worse ratio.
   */
  private static final int LZ4_FAST_ACCELERATION = 2;

  /**
   * Size threshold for adaptive compression (only used in HIGH_COMPRESSION mode).
   * Pages smaller than this use fast mode even in HC mode for latency.
   */
  private static final int ADAPTIVE_HC_THRESHOLD = 16 * 1024; // 16KB

  /**
   * Minimum size to attempt compression. Smaller data has too much overhead.
   */
  private static final int MIN_COMPRESSION_SIZE = 64;

  /**
   * The compression mode for this instance.
   */
  private final CompressionMode compressionMode;

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
    MethodHandle compressFast = null;
    MethodHandle compressHC = null;
    MethodHandle decompress = null;
    MethodHandle decompressFast = null;
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

      // int LZ4_compress_fast(const char* src, char* dst, int srcSize, int dstCapacity, int acceleration)
      // acceleration: 1 = default speed, higher = faster but worse compression
      compressFast = LINKER.downcallHandle(
          lz4Lib.find("LZ4_compress_fast").orElseThrow(),
          FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, JAVA_INT)
      );

      // int LZ4_compress_HC(const char* src, char* dst, int srcSize, int dstCapacity, int compressionLevel)
      compressHC = LINKER.downcallHandle(
          lz4Lib.find("LZ4_compress_HC").orElseThrow(),
          FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, JAVA_INT)
      );

      // int LZ4_decompress_safe(const char* src, char* dst, int compressedSize, int dstCapacity)
      decompress = LINKER.downcallHandle(
          lz4Lib.find("LZ4_decompress_safe").orElseThrow(),
          FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT)
      );

      // int LZ4_decompress_fast(const char* src, char* dst, int originalSize)
      // DEPRECATED but ~10-15% faster - doesn't validate compressed buffer bounds
      // Returns: number of bytes read from src, or negative on error
      decompressFast = LINKER.downcallHandle(
          lz4Lib.find("LZ4_decompress_fast").orElseThrow(),
          FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT)
      );

      // int LZ4_compressBound(int inputSize)
      compressBound = LINKER.downcallHandle(
          lz4Lib.find("LZ4_compressBound").orElseThrow(),
          FunctionDescriptor.of(JAVA_INT, JAVA_INT)
      );

      available = true;
      LOGGER.info("Native LZ4 library loaded successfully via FFI (with fast and HC modes)");
    } catch (Exception e) {
      LOGGER.warn("Native LZ4 library not available, falling back to lz4-java: {}", e.getMessage());
    }

    NATIVE_LZ4_AVAILABLE = available;
    LZ4_COMPRESS_DEFAULT = compress;
    LZ4_COMPRESS_FAST = compressFast;
    LZ4_COMPRESS_HC = compressHC;
    LZ4_DECOMPRESS_SAFE = decompress;
    LZ4_DECOMPRESS_FAST = decompressFast;
    LZ4_COMPRESS_BOUND = compressBound;
    
    if (USE_FAST_DECOMPRESS && available) {
      LOGGER.info("LZ4 fast decompression enabled (sirix.lz4.fast.decompress=true)");
    }
  }

  /**
   * Creates a new FFILz4Compressor with FAST compression mode (default).
   * Best for write-heavy workloads like bulk imports and shredding.
   */
  public FFILz4Compressor() {
    this(CompressionMode.FAST);
  }

  /**
   * Creates a new FFILz4Compressor with the specified compression mode.
   *
   * @param compressionMode the compression mode to use
   */
  public FFILz4Compressor(CompressionMode compressionMode) {
    this.compressionMode = compressionMode;
    LOGGER.debug("FFILz4Compressor initialized with {} mode", compressionMode);
  }

  /**
   * Returns the compression mode for this instance.
   *
   * @return the compression mode
   */
  public CompressionMode getCompressionMode() {
    return compressionMode;
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
   * Zero-copy operation using FFI with fast (default) compression.
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
   * Compress data with accelerated fast mode for latency-sensitive operations.
   * Uses LZ4_compress_fast which is faster than default at the cost of ~3% worse ratio.
   *
   * @param source uncompressed data
   * @param destination buffer for compressed data (must be large enough)
   * @param acceleration acceleration factor (1=default, higher=faster but worse ratio)
   * @return number of compressed bytes written, or negative on error
   */
  public int compressSegmentFast(MemorySegment source, MemorySegment destination, int acceleration) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    try {
      return (int) LZ4_COMPRESS_FAST.invokeExact(
          source,
          destination,
          (int) source.byteSize(),
          (int) destination.byteSize(),
          acceleration
      );
    } catch (Throwable e) {
      throw new RuntimeException("LZ4 fast compression failed", e);
    }
  }

  /**
   * Compress data from source MemorySegment to destination MemorySegment using High Compression mode.
   * This provides better compression ratios than the default mode at the cost of slower compression.
   * Zero-copy operation using FFI.
   *
   * @param source uncompressed data
   * @param destination buffer for compressed data (must be large enough)
   * @param compressionLevel compression level (1-12, where 9 is default, higher = better compression but slower)
   * @return number of compressed bytes written, or negative on error
   */
  public int compressSegmentHC(MemorySegment source, MemorySegment destination, int compressionLevel) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    try {
      return (int) LZ4_COMPRESS_HC.invokeExact(
          source,
          destination,
          (int) source.byteSize(),
          (int) destination.byteSize(),
          compressionLevel
      );
    } catch (Throwable e) {
      throw new RuntimeException("LZ4 HC compression failed", e);
    }
  }

  /**
   * Decompress data from source MemorySegment to destination MemorySegment.
   * Zero-copy operation using FFI. Uses safe mode (validates bounds).
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
   * Decompress data using fast mode (deprecated but ~10-15% faster).
   * 
   * <p><b>WARNING:</b> This doesn't validate compressed buffer bounds.
   * Only use when data is from trusted source (like Sirix storage).
   *
   * @param source compressed data
   * @param destination buffer for decompressed data (must be exactly originalSize)
   * @param originalSize the expected decompressed size
   * @return number of bytes read from source, or negative on error
   */
  public int decompressSegmentFast(MemorySegment source, MemorySegment destination, int originalSize) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    try {
      return (int) LZ4_DECOMPRESS_FAST.invokeExact(
          source,
          destination,
          originalSize
      );
    } catch (Throwable e) {
      throw new RuntimeException("LZ4 fast decompression failed", e);
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
   * <p>Compression strategy depends on the configured {@link CompressionMode}:
   * <ul>
   *   <li>Data smaller than MIN_COMPRESSION_SIZE (64 bytes): returned as-is with header</li>
   *   <li>{@link CompressionMode#FAST}: always uses LZ4 fast mode (~10x faster than HC)</li>
   *   <li>{@link CompressionMode#HIGH_COMPRESSION}: uses fast mode for small data (&lt;16KB),
   *       HC mode for larger data (better compression ratio)</li>
   * </ul>
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
    
    // Skip compression for very small data - overhead exceeds benefit
    if (srcSize < MIN_COMPRESSION_SIZE) {
      // Return uncompressed with header (negative size indicates uncompressed)
      MemorySegment result = Arena.ofAuto().allocate(srcSize + 4);
      result.set(JAVA_INT, 0, -srcSize); // Negative size = uncompressed
      MemorySegment.copy(source, 0, result, 4, srcSize);
      return result;
    }
    
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

      // Choose compression based on configured mode
      int compressedSize;
      if (compressionMode == CompressionMode.FAST) {
        // Always use fast mode - optimized for write throughput
        compressedSize = compressSegmentFast(nativeSource, tempCompressed.asSlice(4), LZ4_FAST_ACCELERATION);
        if (compressedSize <= 0) {
          throw new RuntimeException("LZ4 fast compression failed: " + compressedSize);
        }
      } else {
        // HIGH_COMPRESSION mode: use adaptive approach
        // Small pages still use fast mode for latency, large pages use HC
        if (srcSize < ADAPTIVE_HC_THRESHOLD) {
          compressedSize = compressSegmentFast(nativeSource, tempCompressed.asSlice(4), LZ4_FAST_ACCELERATION);
          if (compressedSize <= 0) {
            throw new RuntimeException("LZ4 fast compression failed: " + compressedSize);
          }
        } else {
          compressedSize = compressSegmentHC(nativeSource, tempCompressed.asSlice(4), LZ4HC_CLEVEL_DEFAULT);
          if (compressedSize <= 0) {
            throw new RuntimeException("LZ4 HC compression failed: " + compressedSize);
          }
        }
      }
      
      // Check if compression is actually beneficial (at least 5% savings)
      // If not, store uncompressed to avoid decompression overhead
      int totalCompressedSize = compressedSize + 4;
      if (totalCompressedSize >= srcSize * 0.95) {
        // Compression not beneficial, store uncompressed
        MemorySegment result = Arena.ofAuto().allocate(srcSize + 4);
        result.set(JAVA_INT, 0, -srcSize); // Negative size = uncompressed
        MemorySegment.copy(nativeSource, 0, result, 4, srcSize);
        return result;
      }

      // Copy to auto arena result that persists
      MemorySegment result = Arena.ofAuto().allocate(totalCompressedSize);
      MemorySegment.copy(tempCompressed, 0, result, 0, totalCompressedSize);
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

    int sizeHeader = compressed.get(JAVA_INT_UNALIGNED, 0);
    
    // Negative size indicates uncompressed data (stored as-is)
    if (sizeHeader < 0) {
      int uncompressedSize = -sizeHeader;
      MemorySegment buffer = Arena.ofAuto().allocate(uncompressedSize);
      MemorySegment.copy(compressed, 4, buffer, 0, uncompressedSize);
      return buffer;
    }
    
    int decompressedSize = sizeHeader;
    
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
   * Decompress data using the unified allocator for zero-copy page support.
   * 
   * <p>Unlike the pool-based approach, this allocates from the MemorySegmentAllocator
   * which allows buffer lifetime to match page lifetime for zero-copy deserialization.
   * When a page takes ownership via {@link DecompressionResult#transferOwnership()},
   * the buffer becomes the page's slotMemory directly without copying.
   * 
   * <p>Memory is still bounded because the allocator uses tiered pools internally.
   * 
   * <p>The returned result MUST be closed after use (unless ownership is transferred):
   * <pre>{@code
   * try (var result = compressor.decompressScoped(compressed)) {
   *     Page page = deserialize(result.segment());
   * } // Buffer returned to allocator
   * }</pre>
   *
   * @param compressed compressed data (with 4-byte header containing decompressed size)
   * @return scoped result; must be closed to return buffer to allocator
   */
  @Override
  public DecompressionResult decompressScoped(MemorySegment compressed) {
    if (!NATIVE_LZ4_AVAILABLE) {
      throw new UnsupportedOperationException("Native LZ4 not available");
    }

    int sizeHeader = compressed.get(JAVA_INT_UNALIGNED, 0);
    
    // Negative size indicates uncompressed data (stored as-is for small/incompressible data)
    if (sizeHeader < 0) {
      int uncompressedSize = -sizeHeader;
      MemorySegment buffer = ALLOCATOR.allocate(uncompressedSize);
      MemorySegment.copy(compressed, 4, buffer, 0, uncompressedSize);
      
      final MemorySegment backingBuffer = buffer;
      return new DecompressionResult(
          buffer,                           // segment
          backingBuffer,                    // backingBuffer (same as segment)
          () -> ALLOCATOR.release(backingBuffer),  // releaser
          new java.util.concurrent.atomic.AtomicBoolean(false)  // ownershipTransferred
      );
    }
    
    int decompressedSize = sizeHeader;
    
    // Use unified allocator - buffer lifetime matches page lifetime for zero-copy
    // This replaces the pool-based approach to enable ownership transfer
    MemorySegment buffer = ALLOCATOR.allocate(decompressedSize);

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
      // Decompress - choose between safe (validated) and fast (unvalidated) modes
      int actualSize;
      if (USE_FAST_DECOMPRESS) {
        // Fast mode: ~10-15% faster, doesn't validate compressed buffer bounds
        // Returns bytes read from source (not written to dest)
        int bytesRead = decompressSegmentFast(
            nativeCompressed.asSlice(4),
            buffer,
            decompressedSize
        );
        if (bytesRead < 0) {
          ALLOCATOR.release(buffer);
          throw new RuntimeException("LZ4 fast decompression failed: " + bytesRead);
        }
        actualSize = decompressedSize; // Fast mode always writes exactly originalSize bytes
      } else {
        // Safe mode: validates compressed buffer bounds
        actualSize = decompressSegment(
            nativeCompressed.asSlice(4),
            buffer,
            (int) nativeCompressed.byteSize() - 4
        );
        if (actualSize < 0) {
          ALLOCATOR.release(buffer);
          throw new RuntimeException("LZ4 decompression failed: " + actualSize);
        }
      }

      if (actualSize != decompressedSize) {
        LOGGER.warn("Decompressed size mismatch: expected {}, got {}", decompressedSize, actualSize);
      }

      // Create result with:
      // - segment: the actual decompressed data slice
      // - backingBuffer: the full buffer for zero-copy ownership transfer
      // - releaser: returns buffer to allocator (called unless ownership transferred)
      // - ownershipTransferred: tracks if page took ownership
      final MemorySegment backingBuffer = buffer;
      
      return new DecompressionResult(
          buffer.asSlice(0, actualSize),   // segment (may be smaller than allocated)
          backingBuffer,                    // backingBuffer (full allocation)
          () -> ALLOCATOR.release(backingBuffer),  // releaser
          new java.util.concurrent.atomic.AtomicBoolean(false)  // ownershipTransferred
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

