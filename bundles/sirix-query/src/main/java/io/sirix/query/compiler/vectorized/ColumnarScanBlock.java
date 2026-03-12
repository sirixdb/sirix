package io.sirix.query.compiler.vectorized;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Str;
import io.brackit.query.block.Block;
import io.brackit.query.block.Sink;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.operator.TupleImpl;
import io.sirix.api.StorageEngineReader;

/**
 * Brackit {@link Block} that drives a {@link ColumnarScanAxis} to produce
 * columnar batches of string-type nodes, applies SIMD filters, materializes
 * surviving strings, and emits Tuple[] downstream.
 *
 * <p>This block is a **source block** — it generates tuples from the storage
 * engine rather than transforming an upstream source. It should be the first
 * block in a {@link io.brackit.query.block.BlockChain}.</p>
 *
 * <h3>Output tuple layout</h3>
 * <ul>
 *   <li>Column 0: {@link Int64} — absolute node key</li>
 *   <li>Column 1: {@link Str} — materialized string value</li>
 * </ul>
 *
 * <p>The block supports optional SIMD pre-filters on numeric columns
 * (node key, parent key) via {@link ColumnBatchFilter} before materializing
 * strings — enabling late materialization where only surviving rows are
 * FSST-decoded.</p>
 */
public final class ColumnarScanBlock implements Block {

  /** Output tuple width: nodeKey + stringValue. */
  private static final int OUTPUT_WIDTH = 2;

  /** Cached empty string to avoid per-row allocation for null values. */
  private static final Str EMPTY_STR = new Str("");

  private final StorageEngineReader reader;
  private final boolean extractDeweyIds;

  // Optional pre-filter on parentKey (null = no filter)
  private final ComparisonOperator parentKeyOp;
  private final long parentKeyConstant;

  /**
   * Create a columnar scan block without any pre-filters.
   *
   * @param reader          the storage engine reader for page resolution
   * @param extractDeweyIds whether to extract DeweyIDs
   */
  public ColumnarScanBlock(final StorageEngineReader reader, final boolean extractDeweyIds) {
    this(reader, extractDeweyIds, null, 0);
  }

  /**
   * Create a columnar scan block with an optional parent key pre-filter.
   *
   * @param reader            the storage engine reader
   * @param extractDeweyIds   whether to extract DeweyIDs
   * @param parentKeyOp       comparison operator for parent key filter (null = no filter)
   * @param parentKeyConstant constant for parent key comparison
   */
  public ColumnarScanBlock(final StorageEngineReader reader, final boolean extractDeweyIds,
      final ComparisonOperator parentKeyOp, final long parentKeyConstant) {
    this.reader = reader;
    this.extractDeweyIds = extractDeweyIds;
    this.parentKeyOp = parentKeyOp;
    this.parentKeyConstant = parentKeyConstant;
  }

  @Override
  public Sink create(final QueryContext ctx, final Sink sink) throws QueryException {
    return new ColumnarScanSink(ctx, sink);
  }

  @Override
  public int outputWidth(final int inputWidth) {
    return OUTPUT_WIDTH;
  }

  private final class ColumnarScanSink implements Sink {
    private final QueryContext ctx;
    private final Sink downstream;
    private ColumnarScanAxis axis;
    private Tuple[] tupleBuffer;
    // Pre-allocated Sequence[] arrays — reused across batches to reduce GC pressure
    private Sequence[][] sequenceArrays;

    ColumnarScanSink(final QueryContext ctx, final Sink downstream) {
      this.ctx = ctx;
      this.downstream = downstream;
    }

    @Override
    public void output(final Tuple[] buf, final int len) throws QueryException {
      // Source block — no upstream input. Data is produced in end().
    }

    @Override
    public void begin() throws QueryException {
      axis = new ColumnarScanAxis(reader, extractDeweyIds);
      tupleBuffer = new Tuple[ColumnBatch.DEFAULT_CAPACITY];
      sequenceArrays = new Sequence[ColumnBatch.DEFAULT_CAPACITY][OUTPUT_WIDTH];
      downstream.begin();
    }

    @Override
    public void end() throws QueryException {
      try {
        driveFullScan();
      } finally {
        if (axis != null) {
          axis.close();
          axis = null;
        }
      }
      downstream.end();
    }

    private void driveFullScan() throws QueryException {
      ColumnBatch batch;
      while ((batch = axis.nextBatch()) != null) {
        // Apply optional parent key pre-filter (SIMD on INT64)
        if (parentKeyOp != null) {
          ColumnBatchFilter.filterLong(batch, ColumnarScanAxis.COL_PARENT_KEY,
              parentKeyOp, parentKeyConstant);
        }

        if (batch.selectionCount() == 0) {
          continue;
        }

        // Late materialization: only decode strings that survived the filter
        batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE);

        // Convert surviving rows to Tuple[] using pre-allocated Sequence arrays
        final int count = batch.selectionCount();
        final int[] sel = batch.selectionVector();
        for (int i = 0; i < count; i++) {
          final int row = sel[i];
          final long nodeKey = batch.getLong(ColumnarScanAxis.COL_NODE_KEY, row);
          final String value = batch.getString(ColumnarScanAxis.COL_STRING_VALUE, row);
          final Sequence[] cols = sequenceArrays[i];
          cols[0] = new Int64(nodeKey);
          cols[1] = value != null ? new Str(value) : EMPTY_STR;
          tupleBuffer[i] = new TupleImpl(cols);
        }

        if (count > 0) {
          downstream.output(tupleBuffer, count);
        }
      }
    }

    @Override
    public void fail() throws QueryException {
      if (axis != null) {
        axis.close();
        axis = null;
      }
      downstream.fail();
    }

    @Override
    public Sink fork() {
      return new ColumnarScanSink(ctx, downstream.fork());
    }

    @Override
    public Sink partition(final Sink stopAt) {
      return new ColumnarScanSink(ctx, downstream.partition(stopAt));
    }
  }
}
