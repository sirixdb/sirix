package io.sirix.query.compiler.translator;

import io.sirix.api.Axis;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.AbstractAxis;
import io.sirix.node.SirixDeweyID;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import static java.util.Objects.requireNonNull;

/**
 * Merges forward, document-ordered structural axes sharing one transaction. Descendant plans must
 * merge before positional predicates consume their streams; sorting afterwards is too late. Without
 * Dewey IDs, tree links determine order because inserted node keys need not be monotonic.
 */
final class OrderedUnionAxis extends AbstractAxis {
  private final Axis first;
  private final Axis second;
  private final LongArrayList firstAncestors = new LongArrayList(16);
  private final LongArrayList secondAncestors = new LongArrayList(16);
  private final LongArrayList previousAncestors = new LongArrayList(16);

  @SuppressWarnings("ReferenceEquality") // Operands must share the exact mutable cursor instance.
  OrderedUnionAxis(final XmlNodeReadOnlyTrx rtx, final Axis first, final Axis second) {
    super(rtx);
    this.first = requireNonNull(first);
    this.second = requireNonNull(second);
    if (first.getCursor() != rtx || second.getCursor() != rtx) {
      throw new IllegalArgumentException("Operands must use the merge transaction");
    }
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    if (first != null) {
      first.reset(nodeKey);
    }
    if (second != null) {
      second.reset(nodeKey);
    }
    if (previousAncestors != null) {
      previousAncestors.clear();
    }
  }

  @Override
  protected long nextKey() {
    final boolean hasFirst = first.hasNext();
    final boolean hasSecond = second.hasNext();
    if (!hasFirst) {
      return hasSecond
          ? second.nextLong()
          : done();
    }
    if (!hasSecond) {
      return first.nextLong();
    }
    final long firstKey = first.peek();
    final long secondKey = second.peek();
    if (firstKey == secondKey) {
      first.nextLong();
      return second.nextLong();
    }
    return precedes(firstKey, secondKey)
        ? first.nextLong()
        : second.nextLong();
  }

  private boolean precedes(final long firstKey, final long secondKey) {
    final XmlNodeReadOnlyTrx rtx = asXmlNodeReadTrx();
    rtx.moveTo(firstKey);
    final SirixDeweyID firstId = rtx.getDeweyID();
    if (firstId != null) {
      rtx.moveTo(secondKey);
      return firstId.compareTo(rtx.getDeweyID()) < 0;
    }

    collectAncestors(rtx, firstKey, firstAncestors);
    collectAncestors(rtx, secondKey, secondAncestors);
    int firstIndex = firstAncestors.size() - 1;
    int secondIndex = secondAncestors.size() - 1;
    while (firstIndex >= 0 && secondIndex >= 0
        && firstAncestors.getLong(firstIndex) == secondAncestors.getLong(secondIndex)) {
      firstIndex--;
      secondIndex--;
    }
    final boolean firstPrecedes = (firstIndex < 0 || secondIndex < 0)
        ? firstIndex < secondIndex
        : precedesBranches(rtx, firstIndex, secondIndex);
    final LongArrayList emittedAncestors = firstPrecedes
        ? firstAncestors
        : secondAncestors;
    previousAncestors.clear();
    previousAncestors.addElements(0, emittedAncestors.elements(), 0, emittedAncestors.size());
    return firstPrecedes;
  }

  private boolean precedesBranches(final XmlNodeReadOnlyTrx rtx, final int firstIndex, final int secondIndex) {
    final long firstBranch = firstAncestors.getLong(firstIndex);
    final long secondBranch = secondAncestors.getLong(secondIndex);
    final long parentKey = firstAncestors.getLong(firstIndex + 1);
    // Ordered operands let the sibling scan resume at the previously emitted branch.
    final int previousIndex = previousAncestors.size() - firstAncestors.size() + firstIndex;
    if (previousIndex >= 0 && previousAncestors.getLong(previousIndex + 1) == parentKey) {
      rtx.moveTo(previousAncestors.getLong(previousIndex));
    } else {
      rtx.moveTo(parentKey);
      rtx.moveToFirstChild();
    }
    do {
      final long branchKey = rtx.getNodeKey();
      if (branchKey == firstBranch || branchKey == secondBranch) {
        return branchKey == firstBranch;
      }
    } while (rtx.moveToRightSibling());
    throw new IllegalStateException("Operands must advance in document order");
  }

  private static void collectAncestors(final XmlNodeReadOnlyTrx rtx, final long nodeKey,
      final LongArrayList ancestors) {
    ancestors.clear();
    rtx.moveTo(nodeKey);
    do {
      ancestors.add(rtx.getNodeKey());
    } while (rtx.moveToParent());
  }
}
