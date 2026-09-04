/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Objects;

/**
 * Layout of the EXACT identity a composite group-by stores alongside each group in a
 * {@link NumericGroupAggTable}.
 *
 * <h2>Why the probe hash cannot be the identity</h2>
 *
 * The composite kernels fold one 64-bit hash per key component into a single 64-bit group key
 * through {@code h = h * FNV_PRIME ^ componentHash}. That fold is not merely lossy in the usual
 * "birthday bound" sense — it is <em>solvable</em>. Component hashes of a numeric key are
 * {@code HashCommon.mix}, a bijection fastutil ships an explicit inverse for, so for a
 * two-component key one picks the first component freely and computes the second in closed form:
 *
 * <pre>
 * c1 = invMix(((FNV_SEED * FNV_PRIME ^ mix(b0)) * FNV_PRIME) ^ hTarget)
 * </pre>
 *
 * which makes {@code (b0, b1)} collide with any chosen group. Two unrelated groups then fold into
 * one and the query returns a silently wrong answer. Storing the components themselves removes the
 * fold from the identity decision and leaves the hash doing only what a hash should: choosing a
 * probe chain.
 *
 * <h2>Lane layout</h2>
 *
 * <pre>
 *   lane 0                    presence mask — bit k set ⇒ component k had NO value and no
 *                             substitution, so "missing" is part of the identity rather than a
 *                             value that happens to encode as zero
 *   laneOffsets[k] ..         component k's identity, {@link #lanesFor} lanes wide
 * </pre>
 *
 * <h2>Per-component exactness</h2>
 *
 * <ul>
 * <li><b>{@code NUMERIC_LONG}</b> — one lane holding the transformed value itself (offset and
 * div/mod already applied). Byte-exact.</li>
 * <li><b>substring-cast components</b> (any kind with a positive substring start, including
 * {@code STRING_GLOBAL}, which the composite kernels only admit in that shape) — one lane holding
 * the cast {@code xs:integer}. Byte-exact: the cast result <em>is</em> the group.</li>
 * <li><b>per-leaf dictionary strings</b> — two lanes holding a 128-bit content identity, the FNV-1a
 * primary paired with the xxh3 secondary. Per-leaf dictionary ids are not comparable across leaves
 * or across scan workers, and the alternative — interning every leaf's dictionary into scan-global
 * ids — would need a shared mutable interner on a path that 20 workers walk once per dictionary
 * entry. Two independent 64-bit functions over the same bytes keep the identity worker-independent
 * and mergeable with no synchronisation at all.</li>
 * </ul>
 *
 * <p>
 * So the fold is exact, numeric and cast components are exact, and a string component is exact up
 * to a simultaneous collision in two independent 64-bit hashes.
 */
public final class CompositeGroupIdentity {

  /** Widest composite key the presence mask can describe. */
  public static final int MAX_KEY_COMPONENTS = Long.SIZE;

  private CompositeGroupIdentity() {
    throw new AssertionError("no instances");
  }

  /**
   * Whether component {@code k} carries its value through a substring cast, which makes its identity
   * the cast result rather than the source string.
   *
   * @param keySubstr per-component {@code [start, length]} pairs, or {@code null}
   * @param k the component ordinal
   * @return {@code true} when the component is cast
   */
  public static boolean isCast(final int[] keySubstr, final int k) {
    return keySubstr != null && keySubstr[2 * k] > 0;
  }

  /**
   * Identity lanes component {@code k} occupies.
   *
   * @param keyKinds per-component column kinds
   * @param keySubstr per-component {@code [start, length]} pairs, or {@code null}
   * @param k the component ordinal
   * @return {@code 1} for an exactly representable component, {@code 2} for a dictionary string
   */
  public static int lanesFor(final byte[] keyKinds, final int[] keySubstr, final int k) {
    // A temporal component is one exact lane: its epoch is the identity, the same way a numeric
    // component's value is — nothing has to be recovered from a dictionary to compare two of them.
    if (isCast(keySubstr, k) || ProjectionIndexRowGroupPage.isOrderedLongKind(keyKinds[k])
        || keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
      return 1;
    }
    return 2;
  }

  /**
   * First identity lane of each component; the returned array has one extra trailing entry holding
   * the total width, so {@code offsets[keyCount]} is {@link #width}.
   *
   * @param keyKinds per-component column kinds
   * @param keySubstr per-component {@code [start, length]} pairs, or {@code null}
   * @return the lane offsets, index-aligned to the components
   * @throws IllegalArgumentException if the key has more components than the mask can describe
   */
  public static int[] laneOffsets(final byte[] keyKinds, final int[] keySubstr) {
    Objects.requireNonNull(keyKinds, "keyKinds must not be null");
    if (keyKinds.length > MAX_KEY_COMPONENTS) {
      throw new IllegalArgumentException("composite key of " + keyKinds.length + " components exceeds the "
          + MAX_KEY_COMPONENTS + "-bit presence mask");
    }
    final int[] offsets = new int[keyKinds.length + 1];
    int lane = 1; // lane 0 is the presence mask
    for (int k = 0; k < keyKinds.length; k++) {
      offsets[k] = lane;
      lane += lanesFor(keyKinds, keySubstr, k);
    }
    offsets[keyKinds.length] = lane;
    return offsets;
  }

  /**
   * Whether any component is identified by a FINGERPRINT pair rather than by an exact value, i.e.
   * whether the scan needs a {@link ProjectionStringIdentityRegistry} to prove byte equality.
   *
   * @param keyKinds per-component column kinds
   * @param keySubstr per-component {@code [start, length]} pairs, or {@code null}
   * @return {@code true} when at least one component is a non-cast dictionary string
   */
  public static boolean hasFingerprintedComponent(final byte[] keyKinds, final int[] keySubstr) {
    for (int k = 0; k < keyKinds.length; k++) {
      if (lanesFor(keyKinds, keySubstr, k) == 2) {
        return true;
      }
    }
    return false;
  }

  /**
   * Total identity width for a composite key — what a {@link NumericGroupAggTable} must be built with
   * so its stripes can carry the identity.
   *
   * @param keyKinds per-component column kinds
   * @param keySubstr per-component {@code [start, length]} pairs, or {@code null}
   * @return the number of identity lanes per group
   */
  public static int width(final byte[] keyKinds, final int[] keySubstr) {
    return laneOffsets(keyKinds, keySubstr)[keyKinds.length];
  }
}
