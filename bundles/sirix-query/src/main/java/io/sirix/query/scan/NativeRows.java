package io.sirix.query.scan;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * One checked access per row: a 256-bit vector load of four consecutive longs from a
 * process-private slab into a heap scratch, used by kernels over row-major lane blocks and 40-byte
 * packed records. Where the platform's preferred vector is narrower than 256 bits the load would be
 * emulated lane by lane, so {@link #VECTOR_ROWS} is false there and callers read scalars instead.
 */
public final class NativeRows {
  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_256;
  private static final ByteOrder ORDER = ByteOrder.nativeOrder();
  /** Whether a four-long row load compiles to one vector instruction here. */
  public static final boolean VECTOR_ROWS = LongVector.SPECIES_PREFERRED.length() >= SPECIES.length();
  /** Longs per row load; scratch arrays must hold at least this many. */
  public static final int LANES = 4;

  private NativeRows() {}

  /** Load four longs at {@code offset} into {@code into[0..3]} with one bounds check. */
  public static void load(final MemorySegment slab, final long offset, final long[] into) {
    LongVector.fromMemorySegment(SPECIES, slab, offset, ORDER).intoArray(into, 0);
  }
}
