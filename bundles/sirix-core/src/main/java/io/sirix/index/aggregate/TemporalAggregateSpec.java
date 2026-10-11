package io.sirix.index.aggregate;

import java.util.List;
import java.util.Objects;

/**
 * Immutable declaration of aggregate lanes over a covering projection. A lane contains either one
 * integer column or the checked product of two integer columns. The declaration records the
 * required operations so sum-only lanes do not maintain extrema. The index also supplies the number
 * of records in each group.
 */
public record TemporalAggregateSpec(int projectionId, List<Integer> groupColumns, List<Lane> lanes) {
  public static final int COUNT = 1;
  public static final int SUM = 2;
  public static final int MIN = 4;
  public static final int MAX = 8;
  public static final int ALL = COUNT | SUM | MIN | MAX;

  public static int operation(final String function) {
    return switch (function) {
      case "count" -> COUNT;
      case "sum" -> SUM;
      case "min" -> MIN;
      case "max" -> MAX;
      default -> throw new IllegalArgumentException("Unsupported aggregate: " + function);
    };
  }

  public record Lane(int left, int right, int operations) {
    public Lane {
      if (left < 0 || right < -1 || operations <= 0 || (operations & ~ALL) != 0) {
        throw new IllegalArgumentException("Invalid aggregate column");
      }
      if (right >= 0 && left > right) {
        final int swap = left;
        left = right;
        right = swap;
      }
    }

    public Lane(final int column) {
      this(column, -1, ALL);
    }

    public Lane(final int left, final int right) {
      this(left, right, ALL);
    }

    public boolean hasExtrema() {
      return (operations & (MIN | MAX)) != 0;
    }

    public boolean sameOperand(final Lane other) {
      return left == other.left && right == other.right;
    }
  }

  public TemporalAggregateSpec {
    if (projectionId < 0) {
      throw new IllegalArgumentException("Negative projection id");
    }
    groupColumns = List.copyOf(Objects.requireNonNull(groupColumns));
    lanes = List.copyOf(Objects.requireNonNull(lanes));
    if (groupColumns.isEmpty() || groupColumns.size() > 16 || lanes.size() > 16) {
      throw new IllegalArgumentException("An aggregate index needs 1..16 grouping columns and at most 16 lanes");
    }
    for (int i = 0; i < lanes.size(); i++) {
      for (int j = 0; j < i; j++) {
        if (lanes.get(i).sameOperand(lanes.get(j))) {
          throw new IllegalArgumentException("Duplicate aggregate operand");
        }
      }
    }
    for (int i = 0; i < groupColumns.size(); i++) {
      final int column = groupColumns.get(i);
      if (column < 0) {
        throw new IllegalArgumentException("Negative grouping column");
      }
      for (int j = 0; j < i; j++) {
        if (column == groupColumns.get(j)) {
          throw new IllegalArgumentException("Duplicate aggregate group column");
        }
      }
    }
  }

  public void validateColumnCount(final int columns) {
    for (final int column : groupColumns) {
      if (column >= columns) {
        throw new IllegalArgumentException("Grouping column outside the projection: " + column);
      }
    }
    for (final Lane lane : lanes) {
      if (lane.left() >= columns || lane.right() >= columns) {
        throw new IllegalArgumentException("Aggregate lane outside the projection: " + lane);
      }
    }
  }
}
