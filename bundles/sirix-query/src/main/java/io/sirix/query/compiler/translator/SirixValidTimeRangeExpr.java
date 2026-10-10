package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.util.ExprUtil;
import io.sirix.access.ValidTimeConfig;
import io.sirix.query.function.jn.temporal.ValidTimeFilter;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.RoutedKeys;
import io.sirix.query.json.JsonDBItem;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Arrays;

/** Internal range source with revision-scoped exact admission and the original lazy comparisons. */
final class SirixValidTimeRangeExpr implements Expr {
  private final StaticContext context;
  private final Expr document;
  private final Expr startLimit;
  private final Expr endLimit;
  private final String from;
  private final String to;
  private final int mode;
  private final @Nullable Expr baseSource;

  SirixValidTimeRangeExpr(final StaticContext context, final Expr document, final Expr startLimit, final Expr endLimit,
      final String from, final String to, final int mode, final @Nullable Expr baseSource) {
    this.context = Objects.requireNonNull(context);
    this.document = Objects.requireNonNull(document);
    this.startLimit = Objects.requireNonNull(startLimit);
    this.endLimit = Objects.requireNonNull(endLimit);
    this.from = Objects.requireNonNull(from);
    this.to = Objects.requireNonNull(to);
    if (mode < 0 || mode > 127) {
      throw new IllegalArgumentException("Invalid range comparison mode: " + mode);
    }
    this.mode = mode;
    this.baseSource = baseSource;
  }

  private @Nullable Sequence indexed(final JsonDBItem source, final QueryContext ctx, final Tuple tuple) {
    final ValidTimeConfig config = source.getResourceSession().getResourceConfig().getValidTimeConfig();
    return config != null && from.equals(config.getNormalizedValidFromPath())
        && to.equals(config.getNormalizedValidToPath())
            ? ValidTimeIntervalIndex.comparisonRangeSequence(source, () -> startLimit.evaluate(ctx, tuple),
                () -> endLimit.evaluate(ctx, tuple), config, (mode & 1) != 0, (mode & 2) != 0)
            : null;
  }

  @Nullable
  RoutedKeys routedKeys(final QueryContext ctx, final Tuple tuple) {
    final RoutedKeys base = baseSource == null
        ? null
        : baseKeys(ctx, tuple);
    if (baseSource != null && base == null) {
      return null;
    }
    if (base != null && base.keys().length == 0) {
      return base;
    }
    final Item item = document.evaluateToItem(ctx, tuple);
    if (!(item instanceof JsonDBItem source) || !(source instanceof Array array)) {
      return null;
    }
    final RoutedKeys range = array.len() == 0
        ? new RoutedKeys(new long[0], source.getTrx().getRevisionNumber())
        : ValidTimeIntervalIndex.routedKeys(indexed(source, ctx, tuple));
    if (range == null || base == null) {
      return range;
    }
    if (range.revision() != base.revision()) {
      return null;
    }
    final long[] left = base.keys();
    final long[] right = range.keys();
    final long[] intersection = new long[Math.min(left.length, right.length)];
    int a = 0;
    int b = 0;
    int count = 0;
    while (a < left.length && b < right.length) {
      if (left[a] < right[b]) {
        a++;
      } else if (left[a] > right[b]) {
        b++;
      } else {
        intersection[count++] = left[a++];
        b++;
      }
    }
    return new RoutedKeys(count == intersection.length
        ? intersection
        : Arrays.copyOf(intersection, count), range.revision());
  }

  private @Nullable RoutedKeys baseKeys(final QueryContext ctx, final Tuple tuple) {
    if (baseSource instanceof SirixValidTimeScanExpr scan) {
      return scan.routedKeys(ctx, tuple);
    }
    if (baseSource instanceof SirixValidTimeRangeExpr range) {
      return range.routedKeys(ctx, tuple);
    }
    return ValidTimeIntervalIndex.routedKeys(Objects.requireNonNull(baseSource).evaluate(ctx, tuple));
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) {
    if (baseSource != null) {
      return ValidTimeFilter.comparisonScanSequence(baseSource.evaluate(ctx, tuple),
          () -> startLimit.evaluate(ctx, tuple), () -> endLimit.evaluate(ctx, tuple), from, to, mode, context, ctx);
    }
    final Item item = document.evaluateToItem(ctx, tuple);
    if (!(item instanceof JsonDBItem source) || !(source instanceof Array array) || array.len() == 0) {
      return new ItemSequence();
    }
    return new AbstractSequence() {
      private @Nullable Sequence selected;

      private Sequence selected() {
        if (selected == null) {
          final Sequence indexed = indexed(source, ctx, tuple);
          selected = indexed != null
              ? indexed
              : ValidTimeFilter.comparisonScanSequence(source, () -> startLimit.evaluate(ctx, tuple),
                  () -> endLimit.evaluate(ctx, tuple), from, to, mode, context, ctx);
        }
        return selected;
      }

      @Override
      public boolean isRepeatable() {
        return selected != null && selected.isRepeatable();
      }

      @Override
      public @Nullable IntNumeric knownSize() {
        return selected == null
            ? null
            : selected.knownSize();
      }

      @Override
      public IntNumeric size() {
        return selected().size();
      }

      @Override
      public boolean booleanValue() {
        return selected().booleanValue();
      }

      @Override
      public @Nullable Item get(final IntNumeric position) {
        return selected().get(position);
      }

      @Override
      public Iter iterate() {
        return new BaseIter() {
          private @Nullable Iter input;
          private boolean closed;

          @Override
          public @Nullable Item next() {
            if (closed) {
              return null;
            }
            if (input == null) {
              input = selected().iterate();
            }
            return input.next();
          }

          @Override
          public void close() {
            closed = true;
            if (input != null) {
              input.close();
            }
          }
        };
      }
    };
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) {
    return ExprUtil.asItem(evaluate(ctx, tuple));
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public boolean isVacuous() {
    return false;
  }
}
