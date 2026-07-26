package io.sirix.index.path.summary;

import io.brackit.query.atomic.QNm;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dense SIMD-friendly index from {@code localName} to the {@link PathNode}s whose
 * QNm carries that local name. Built lazily from {@link PathSummaryReader#qnmMapping}
 * on first lookup, then reused until a mutation invalidates it.
 *
 * <p>Hot-path query: {@link SirixVectorizedExecutor} calls
 * {@link PathSummaryReader#findPathsByLocalName(String)} per query field; that scan
 * used to walk the whole {@code qnmMapping} entrySet with a {@code String.equals} per
 * entry. This index reduces it to a SIMD-int-equals over a dense {@code int[]} of
 * pre-hashed {@code localName.hashCode()} keys, branchless on the no-match positions.
 *
 * <h2>HFT properties</h2>
 * <ul>
 *   <li>Single linear scan over a contiguous {@code int[]} — cache-friendly.</li>
 *   <li>Vector-API path uses {@link IntVector#SPECIES_PREFERRED} (8 lanes on
 *       AVX2 / 16 on AVX-512) with {@code compare(EQ)} → mask. Tail handled scalar.</li>
 *   <li>Hash hits are verified via a single {@link String#equals} per matched
 *       slot (collisions are vanishingly rare on real workloads).</li>
 *   <li>Empty-result lookups allocate nothing — the hot {@code containsLocalName}
 *       fast path returns a {@code boolean} without a result list.</li>
 *   <li>Build cost is paid once and amortised across lookups; mutators on
 *       {@code qnmMapping} call {@link #invalidate()} so the index rebuilds lazily.</li>
 * </ul>
 */
final class PathLocalNameIndex {

  private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
  private static final int LANES = INT_SPECIES.length();

  /**
   * Parallel arrays — entry {@code i} pairs {@code localNameKeys[i]} (the
   * {@code String#hashCode} of the localName) with {@code paths[i]} (the set of
   * PathNodes whose QNm has that localName). {@code localNames[i]} is the actual
   * localName string, retained for collision verification.
   *
   * <p>{@code null} signals "not yet built" — the caller must rebuild from
   * {@code qnmMapping} before the next lookup.
   */
  private int @Nullable [] localNameKeys;
  private String @Nullable [] localNames;
  private Set<PathNode> @Nullable [] paths;

  /**
   * Build (or rebuild) the dense arrays from the supplied qnmMapping. Idempotent —
   * subsequent calls with the same mapping reproduce the same arrays in the same
   * order (HashMap iteration order is undefined but stable within a single instance).
   */
  void build(final Map<QNm, Set<PathNode>> qnmMapping) {
    final int n = qnmMapping.size();
    final int[] keys = new int[n];
    final String[] names = new String[n];
    @SuppressWarnings("unchecked")
    final Set<PathNode>[] arr = (Set<PathNode>[]) new Set<?>[n];
    int i = 0;
    for (final Map.Entry<QNm, Set<PathNode>> e : qnmMapping.entrySet()) {
      final String ln = e.getKey().getLocalName();
      // Pre-hash with the same function used at lookup time. Empty/null collapses
      // to 0 (String.hashCode() of "" is 0); callers guard against null lookups.
      keys[i] = ln == null ? 0 : ln.hashCode();
      names[i] = ln;
      arr[i] = e.getValue();
      i++;
    }
    this.localNameKeys = keys;
    this.localNames = names;
    this.paths = arr;
  }

  /** Drop the dense arrays — next lookup will rebuild from qnmMapping. */
  void invalidate() {
    this.localNameKeys = null;
    this.localNames = null;
    this.paths = null;
  }

  boolean isBuilt() {
    return localNameKeys != null;
  }

  /**
   * SIMD-scan {@link #localNameKeys} for {@code String.hashCode()} of {@code localName},
   * union the {@code paths[i]} sets at matching positions whose stored localName equals
   * the query (collision-safe), return the merged list. Returns {@link List#of()} (the
   * shared empty list, no allocation) when no match is found.
   */
  List<PathNode> findByLocalName(final String localName) {
    final int[] keys = localNameKeys;
    final String[] names = localNames;
    final Set<PathNode>[] arr = paths;
    if (keys == null || names == null || arr == null) {
      return List.of();
    }
    final int n = keys.length;
    if (n == 0) {
      return List.of();
    }
    final int target = localName.hashCode();
    final IntVector vTarget = IntVector.broadcast(INT_SPECIES, target);

    List<PathNode> result = null;
    int i = 0;
    final int upper = INT_SPECIES.loopBound(n);
    for (; i < upper; i += LANES) {
      final IntVector v = IntVector.fromArray(INT_SPECIES, keys, i);
      final VectorMask<Integer> mask = v.compare(VectorOperators.EQ, vTarget);
      if (!mask.anyTrue()) {
        continue;
      }
      // Resolve hits in this lane block.
      long bits = mask.toLong();
      while (bits != 0) {
        final int lane = Long.numberOfTrailingZeros(bits);
        bits &= bits - 1L;
        final int pos = i + lane;
        if (localName.equals(names[pos])) {
          final Set<PathNode> hit = arr[pos];
          if (result == null) {
            result = new ArrayList<>(hit.size());
          }
          result.addAll(hit);
        }
      }
    }
    // Scalar tail.
    for (; i < n; i++) {
      if (keys[i] == target && localName.equals(names[i])) {
        final Set<PathNode> hit = arr[i];
        if (result == null) {
          result = new ArrayList<>(hit.size());
        }
        result.addAll(hit);
      }
    }
    return result == null ? List.of() : result;
  }

  /**
   * Existence-check fast path — same SIMD scan as {@link #findByLocalName} but
   * stops on the first verified hit. Allocates nothing; returns boolean.
   */
  boolean containsLocalName(final String localName) {
    final int[] keys = localNameKeys;
    final String[] names = localNames;
    if (keys == null || names == null) {
      return false;
    }
    final int n = keys.length;
    if (n == 0) {
      return false;
    }
    final int target = localName.hashCode();
    final IntVector vTarget = IntVector.broadcast(INT_SPECIES, target);

    int i = 0;
    final int upper = INT_SPECIES.loopBound(n);
    for (; i < upper; i += LANES) {
      final IntVector v = IntVector.fromArray(INT_SPECIES, keys, i);
      final VectorMask<Integer> mask = v.compare(VectorOperators.EQ, vTarget);
      if (!mask.anyTrue()) {
        continue;
      }
      long bits = mask.toLong();
      while (bits != 0) {
        final int lane = Long.numberOfTrailingZeros(bits);
        bits &= bits - 1L;
        if (localName.equals(names[i + lane])) {
          return true;
        }
      }
    }
    for (; i < n; i++) {
      if (keys[i] == target && localName.equals(names[i])) {
        return true;
      }
    }
    return false;
  }
}
