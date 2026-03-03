package io.sirix.diff.algorithm.fmse;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import io.sirix.node.NodeKind;
import io.sirix.utils.Pair;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared core matching logic for the FMSE (Fast Match / Edit Script) algorithm. This class is used
 * by both XML ({@code FMSE}) and JSON ({@code JsonFMSE}) implementations, providing the
 * type-agnostic matching algorithm from Chawathe et al. 1996.
 *
 * <p>
 * All methods operate on {@code Map<NodeKind, List<Long>>} label maps and a
 * {@link NodeComparator}{@code <Long>}, making them independent of the underlying tree type.
 * </p>
 */
public final class FMSEAlgorithm {

  private FMSEAlgorithm() {
    throw new AssertionError("No instances");
  }

  /**
   * Core matching algorithm. Groups nodes by label (NodeKind), runs LCS on each group, then greedily
   * matches remaining unmatched nodes.
   *
   * @param oldLabels nodes in tree1, sorted by node type
   * @param newLabels nodes in tree2, sorted by node type
   * @param addMatch  callback accepting two primitive longs (old node key, new node key) to register
   *                  a match — uses {@link LongBiConsumer} to avoid autoboxing
   * @param cmp       comparator that determines node equality
   */
  public static void match(final Map<NodeKind, List<Long>> oldLabels,
      final Map<NodeKind, List<Long>> newLabels, final LongBiConsumer addMatch,
      final NodeComparator<Long> cmp) {
    assert oldLabels != null;
    assert newLabels != null;
    assert addMatch != null;
    assert cmp != null;

    // Early exit if either label map is empty — no possible matches.
    if (oldLabels.isEmpty() || newLabels.isEmpty()) {
      return;
    }

    // Compute the intersection of labels without mutating the input maps' keySets.
    final Set<NodeKind> labels = EnumSet.copyOf(oldLabels.keySet());
    labels.retainAll(newLabels.keySet());

    // 2 - for each label do
    for (final NodeKind label : labels) {
      final List<Long> first = oldLabels.get(label);  // 2(a)
      final List<Long> second = newLabels.get(label); // 2(b)

      // 2(c)
      final List<Pair<Long, Long>> common = Util.longestCommonSubsequence(first, second, cmp);
      // Used to remove the nodes in common from s1 and s2 in step 2(e).
      final LongOpenHashSet seen = new LongOpenHashSet(common.size() * 2);

      // 2(d) - for each pair of nodes in the lcs: add to matching.
      for (final Pair<Long, Long> p : common) {
        addMatch.accept(p.getFirst(), p.getSecond());
        seen.add(p.getFirst().longValue());
        seen.add(p.getSecond().longValue());
      }

      // 2(e) (prepare) - remove nodes in common from s1, s2.
      removeCommonNodes(first, seen);
      removeCommonNodes(second, seen);

      // 2(e) - For each unmatched node x \in s1.
      final Iterator<Long> firstIterator = first.iterator();
      while (firstIterator.hasNext()) {
        final Long firstItem = firstIterator.next();
        boolean firstIter = true;
        // If there is an unmatched node y \in s2.
        final Iterator<Long> secondIterator = second.iterator();
        while (secondIterator.hasNext()) {
          final Long secondItem = secondIterator.next();
          // Such that equal.
          if (cmp.isEqual(firstItem, secondItem)) {
            // 2(e)A
            addMatch.accept(firstItem, secondItem);

            // 2(e)B
            if (firstIter) {
              firstIter = false;
              firstIterator.remove();
            }
            secondIterator.remove();
            break;
          }
        }
      }
    }
  }

  /**
   * Remove nodes that appear in the {@code seen} set from the given list.
   *
   * @param list {@link List} of node keys
   * @param seen {@link LongOpenHashSet} of node keys to remove — uses fastutil for zero-boxing
   *             membership checks
   */
  public static void removeCommonNodes(final List<Long> list, final LongOpenHashSet seen) {
    assert list != null;
    assert seen != null;

    list.removeIf(key -> seen.contains(key.longValue()));
  }
}
