/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.ValueDictionaryCollisionNode;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHashBucketNode;
import io.sirix.node.ValueDictionaryRadixNode;
import io.sirix.node.ValueDictionaryValueBlockNode;
import io.sirix.node.ValueDictionaryValueBucketNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.NamePage;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

final class GlobalValueDictionaryRadix {

  private static final int PRIMARY_PATH_BYTES = 3;
  private static final int SECONDARY_PATH_BYTES = Long.BYTES;
  private static final int REVERSE_PATH_BYTES = 3;
  private static final int MAX_BUCKET_ENTRIES = 128;
  private static final int MAX_SECONDARY_BUCKET_CHAIN_LENGTH = 64;
  private static final int MAX_COLLISION_TREE_DEPTH = 64;
  private static final int BUCKET_MASK = 0xFF_FFFF;
  private static final int RECORD_STRIDE = GlobalValueDictionary.PERSISTENT_RECORD_STRIDE;

  private GlobalValueDictionaryRadix() {
    throw new AssertionError("no instances");
  }

  record Roots(long forward, long reverse) {
  }

  record ProbeResult(int id, int units) {
  }

  /**
   * Allocation-free upper bound for the transient workspace retained while appending values.
   *
   * <p>
   * The exact persistent key count is computed by {@link #append} after it has inspected the
   * immutable radix paths and is what gets reserved from {@link NamePage}. This method runs before
   * that inspection so a bounded writer can refuse before it allocates the planning maps. It must
   * therefore cover the worst legal shape for the supplied cardinalities: every addition may touch a
   * different radix path, every old direct bucket that overflows can contribute its 128 existing
   * candidates, and every collision insertion may copy a full balanced-tree path. Arithmetic
   * saturates rather than wrapping a large dictionary into an artificially small reservation.
   */
  static long reservationBytesForAppend(final int oldEntryCount, final int additionCount, final long valueBytes,
      final int largestValueBytes) {
    if (oldEntryCount < 0 || additionCount < 0 || valueBytes < 0 || largestValueBytes < 0
        || largestValueBytes > valueBytes || (additionCount == 0 && valueBytes != 0)) {
      throw new IllegalArgumentException("invalid value dictionary append cardinalities");
    }
    if (additionCount == 0) {
      return 0L;
    }
    final long additions = additionCount;
    final long oldCandidates = Math.min((long) oldEntryCount, saturatedMultiply(additions, MAX_BUCKET_ENTRIES));
    final long collisionCandidates = saturatedAdd(additions, oldCandidates);
    final long finalEntries = saturatedAdd(oldEntryCount, additions);
    final int balancedHeightBound = finalEntries <= 1L
        ? 1
        : Math.multiplyExact(2, Long.SIZE - Long.numberOfLeadingZeros(finalEntries));
    final int insertionHeightGrowth = Integer.SIZE - Integer.numberOfLeadingZeros(additionCount);
    final long recordsPerCollisionInsert =
        saturatedAdd(saturatedMultiply(2L, saturatedAdd(balancedHeightBound, insertionHeightGrowth + 1L)), 1L);
    final long collisionRecords = saturatedMultiply(collisionCandidates, recordsPerCollisionInsert);

    final long reverseBuckets = saturatedAdd((additions + 255L) >>> 8, 1L);
    long recordCount = additions; // immutable value entries
    recordCount = saturatedAdd(recordCount, saturatedMultiply(additions, SECONDARY_PATH_BYTES));
    recordCount = saturatedAdd(recordCount, saturatedMultiply(additions, PRIMARY_PATH_BYTES));
    recordCount = saturatedAdd(recordCount, saturatedMultiply(reverseBuckets, REVERSE_PATH_BYTES + 1L));
    recordCount = saturatedAdd(recordCount, collisionRecords);

    final long radixNodes = saturatedAdd(saturatedMultiply(additions, PRIMARY_PATH_BYTES + SECONDARY_PATH_BYTES),
        saturatedMultiply(reverseBuckets, REVERSE_PATH_BYTES));
    final long radixBytes = saturatedMultiply(radixNodes, 2L + Short.BYTES + 16L * (Byte.BYTES + Long.BYTES));
    final long hashAndCollisionBytes =
        saturatedAdd(saturatedMultiply(collisionCandidates, 3L * (2L * Integer.BYTES + 2L * Long.BYTES)),
            saturatedMultiply(additions, 25L));
    final long reverseBytes =
        saturatedAdd(saturatedMultiply(additions, Long.BYTES), saturatedMultiply(reverseBuckets, 2L * Integer.BYTES));
    final long entryBytes = saturatedAdd(valueBytes, saturatedMultiply(additions, Integer.BYTES));
    final long encodedBytes =
        saturatedAdd(entryBytes, saturatedAdd(radixBytes, saturatedAdd(hashAndCollisionBytes, reverseBytes)));
    final long pageAndIntentLogBytes =
        saturatedMultiply(2L, saturatedAdd(encodedBytes, saturatedMultiply(recordCount, RECORD_STRIDE)));
    return saturatedAdd(pageAndIntentLogBytes, saturatedAdd(saturatedMultiply(additions, 64L), largestValueBytes));
  }

  /**
   * Test seam for the reverse plan's maximum-cardinality arithmetic. Keeping this method purely
   * arithmetic is intentional: a reverse append covers one dense id interval and therefore must not
   * materialise an array or boxed set with one element per entry or bucket.
   */
  static long denseReverseRecordCountForTest(final int oldEntryCount, final int additionCount) {
    if (oldEntryCount < 0 || additionCount < 0) {
      throw new IllegalArgumentException("dictionary entry counts must not be negative");
    }
    if (additionCount == 0) {
      return 0L;
    }
    final int finalEntryCount = Math.addExact(oldEntryCount, additionCount);
    final DenseRadixPlan plan = new DenseRadixPlan(oldEntryCount >>> 8, (finalEntryCount - 1) >>> 8);
    return Math.addExact(plan.leafCount(), plan.nodeCount());
  }

  private static long saturatedAdd(final long left, final long right) {
    if (left < 0 || right < 0 || left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    return left + right;
  }

  private static long saturatedMultiply(final long left, final long right) {
    if (left < 0 || right < 0 || left != 0L && right > Long.MAX_VALUE / left) {
      return Long.MAX_VALUE;
    }
    return left * right;
  }

  private static long entryKeyForLocalId(final long runStart, final int localId) {
    if (runStart <= 0L || localId <= 0) {
      throw new IllegalArgumentException("invalid dense dictionary entry key");
    }
    return recordKeyAt(runStart, (long) localId - 1L);
  }

  private static long recordKeyAt(final long runStart, final long recordOffset) {
    if (runStart <= 0L || recordOffset < 0L) {
      throw new IllegalArgumentException("invalid dense dictionary record key");
    }
    return Math.addExact(runStart, Math.multiplyExact(recordOffset, RECORD_STRIDE));
  }

  static Roots append(final long oldForwardRoot, final long oldReverseRoot, final int oldEntryCount,
      final GlobalValueDictionaryWriter additions, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineWriter writer, final TransactionIntentLog log) {
    return append(oldForwardRoot, oldReverseRoot, oldEntryCount, additions, namePage, databaseType, writer, log, true);
  }

  /**
   * Appends {@code additions}, optionally WITHOUT a forward hash index.
   *
   * <p>
   * A rank-ordered dictionary does not need one: "which id holds this value" is a binary search over
   * the reverse index, which is already sorted by value because ids were minted in collation order.
   * Skipping it is not a minor saving — the forward index measured 64.7 B/entry at D = 275K and 173
   * B/entry at D = 2.62M, because each bounded append writes a fresh set of forward radix nodes at
   * new keys and copy-on-write retains every one of them. The caller is responsible for the ordering
   * claim; {@code ValueDictionaryHeaderNode} refuses a zero forward root on any dictionary that is
   * not fully ordered, so a wrong claim fails loudly at the header rather than silently producing a
   * dictionary nothing can probe.
   * </p>
   *
   * @param buildForwardIndex {@code false} only for a rank-ordered build
   */
  static Roots append(final long oldForwardRoot, final long oldReverseRoot, final int oldEntryCount,
      final GlobalValueDictionaryWriter additions, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineWriter writer, final TransactionIntentLog log, final boolean buildForwardIndex) {
    if (additions.entryCount() == 0) {
      return new Roots(oldForwardRoot, oldReverseRoot);
    }
    final int finalEntryCount = Math.addExact(oldEntryCount, additions.entryCount());

    // Left EMPTY when no forward index is wanted: every forward structure below is driven by this
    // map, so the planning loop, the bucket writes and RadixPlan all become no-ops without a
    // second code path to keep in step.
    final TreeMap<Integer, IntList> additionsByPrimary = new TreeMap<>();
    if (buildForwardIndex) {
      for (int localId = 1; localId <= additions.entryCount(); localId++) {
        additionsByPrimary.computeIfAbsent(hashBucket(additions.hashAt(localId)), ignored -> new IntList())
                          .add(localId);
      }
    }
    final int firstReverseBucket = oldEntryCount >>> 8;
    final int lastReverseBucket = (finalEntryCount - 1) >>> 8;

    final OpenTail openTail = findExtendableTail(oldReverseRoot, oldEntryCount, namePage, databaseType, writer);
    final ReverseAppendPlan reversePlan = planReverseAppend(additions, oldEntryCount, openTail);
    long recordCount = reversePlan.recordCount();
    for (final Map.Entry<Integer, IntList> entry : additionsByPrimary.entrySet()) {
      final ForwardPlan plan = planForwardUpdate(oldForwardRoot, oldReverseRoot, oldEntryCount, entry.getKey(),
          entry.getValue(), additions, namePage, databaseType, writer);
      prepareCollisionPlans(plan, oldReverseRoot, oldEntryCount, additions, namePage, databaseType, writer);
      entry.getValue().forwardPlan = plan;
      recordCount = Math.addExact(recordCount, plan.newRecordCount());
      recordCount = Math.addExact(recordCount, plan.collisionRecordCount);
    }
    final RadixPlan forwardRadixPlan = RadixPlan.forBuckets(additionsByPrimary.keySet());
    final DenseRadixPlan reverseRadixPlan = new DenseRadixPlan(firstReverseBucket, lastReverseBucket);
    recordCount = Math.addExact(recordCount, forwardRadixPlan.nodeCount());
    recordCount = Math.addExact(recordCount, Math.addExact(reverseRadixPlan.leafCount(), reverseRadixPlan.nodeCount()));

    additions.ensureAppendWorkspaceFitsBudget(Math
                                                  .addExact(
                                                      estimateWorkspaceBytes(recordCount, additionsByPrimary,
                                                          forwardRadixPlan, reverseRadixPlan, additions),
                                                      // A tail extension COPIES the old run's bytes and offsets into
                                                      // the replacement, so those
                                                      // bytes are live in this append's workspace even though they are
                                                      // not new values.
                                                      reversePlan.extendsTail()
                                                          ? Math.addExact((long) reversePlan.tailPrefixBytes.length,
                                                              (long) reversePlan.tailPrefixOffsets.length
                                                                  * Integer.BYTES)
                                                          : 0L));
    final long reserved = Math.multiplyExact(recordCount, RECORD_STRIDE);
    final long runStart = namePage.reserveProjectionValueDictionaryKeys(databaseType, reserved);
    final KeyCursor cursor = new KeyCursor(runStart, recordCount);

    // Blocks and spills FIRST, so the bucket directory records that follow the forward section stay
    // a dense consecutive run for DenseRadixPlan.
    for (int b = 0; b < reversePlan.blockFirstLocal.length; b++) {
      final int firstLocal = reversePlan.blockFirstLocal[b];
      final boolean extendsTail = b == 0 && reversePlan.extendsTail();
      // Block 0 of a tail-extending append carries the OLD tail's values first and keeps its first
      // id, so the run it replaces is the same run — one record rewritten, not a new run appended.
      final int prefixCount = extendsTail
          ? reversePlan.tailPrefixCount()
          : 0;
      final byte[] prefixBytes = extendsTail
          ? reversePlan.tailPrefixBytes
          : null;
      final int additionCount = reversePlan.blockCounts[b] - prefixCount;
      final int[] offsets = new int[reversePlan.blockCounts[b] + 1];
      int total = 0;
      for (int i = 0; i < prefixCount; i++) {
        total = reversePlan.tailPrefixOffsets[i + 1];
        offsets[i + 1] = total;
      }
      for (int i = 0; i < additionCount; i++) {
        total = Math.addExact(total, additions.valueBytes(firstLocal + i).length);
        offsets[prefixCount + i + 1] = total;
      }
      final byte[] packed = new byte[total];
      int at = 0;
      if (prefixCount > 0) {
        System.arraycopy(prefixBytes, 0, packed, 0, prefixBytes.length);
        at = prefixBytes.length;
      }
      for (int i = 0; i < additionCount; i++) {
        final byte[] value = additions.valueBytes(firstLocal + i);
        System.arraycopy(value, 0, packed, at, value.length);
        at += value.length;
      }
      final long blockKey = cursor.next();
      reversePlan.blockKeys[b] = blockKey;
      final int blockFirstId = extendsTail
          ? reversePlan.tailFirstAbsoluteId
          : Math.addExact(oldEntryCount, firstLocal);
      // Ownership transfers: these arrays are built here and never touched again.
      put(ValueDictionaryValueBlockNode.takeOwnership(blockKey, blockFirstId, offsets, packed), namePage, databaseType,
          writer, log);
    }
    for (int i = 0; i < reversePlan.spillLocals.length; i++) {
      final long spillKey = cursor.next();
      reversePlan.spillKeys[i] = spillKey;
      put(ValueDictionaryEntryNode.takeOwnership(spillKey, additions.valueBytes(reversePlan.spillLocals[i])), namePage,
          databaseType, writer, log);
    }

    for (final IntList primaryGroup : additionsByPrimary.values()) {
      final ForwardPlan plan = primaryGroup.forwardPlan;
      final long newLeaf;
      if (plan.direct != null) {
        final long bucketKey = cursor.next();
        final long[] hashes = plan.direct.hashes();
        final int[] ids = plan.direct.ids();
        sortPairs(hashes, ids, 0, hashes.length - 1);
        put(new ValueDictionaryHashBucketNode(bucketKey, plan.primaryBucket, hashes, ids), namePage, databaseType,
            writer, log);
        newLeaf = bucketKey;
      } else {
        long secondaryRoot = plan.oldSecondaryRoot;
        for (final Map.Entry<Long, Candidates> secondary : plan.secondary.entrySet()) {
          final long secondaryHash = secondary.getKey();
          final int[] path = secondaryPath(secondaryHash);
          final CollisionTree collisionPlan = plan.collisionRoots.get(secondaryHash);
          final long collisionRoot = writeCollisionPlan(collisionPlan, cursor, namePage, databaseType, writer, log, 0);
          secondaryRoot = replaceLeaf(secondaryRoot, ValueDictionaryRadixNode.FORWARD, path, PRIMARY_PATH_BYTES,
              collisionRoot, cursor, namePage, databaseType, writer, log);
        }
        newLeaf = secondaryRoot;
      }
      primaryGroup.writtenLeafKey = newLeaf;
    }
    final long forwardRoot = forwardRadixPlan.write(ValueDictionaryRadixNode.FORWARD, oldForwardRoot,
        additionsByPrimary, cursor, namePage, databaseType, writer, log);

    final long reverseLeafRunStart = cursor.peek();
    // MONOTONIC cursors over the plan's ascending run arrays. The bucket loop used to rescan every
    // planned block and spill for every bucket, which is O(buckets x runs) — quadratic on a large
    // append, and paid entirely to rediscover an ordering the plan already has.
    int planBlockCursor = 0;
    int planSpillCursor = 0;
    for (int bucket = firstReverseBucket;; bucket++) {
      final int firstId = Math.toIntExact(
          Math.addExact(Math.multiplyExact((long) bucket, ValueDictionaryValueBucketNode.VALUES_PER_BUCKET), 1L));
      final int size = Math.min(ValueDictionaryValueBucketNode.VALUES_PER_BUCKET, finalEntryCount - firstId + 1);
      final IntList dirBlockFirst = new IntList();
      final IntList dirBlockCount = new IntList();
      final LongList dirBlockKeys = new LongList();
      final IntList dirSpillIds = new IntList();
      final LongList dirSpillKeys = new LongList();
      final int[] path = reversePath(bucket);
      final LeafResult oldLeaf =
          leafKey(oldReverseRoot, ValueDictionaryRadixNode.REVERSE, path, 0, namePage, databaseType, writer);
      final int expectedOldSize = oldEntryCount < firstId
          ? 0
          : Math.min(ValueDictionaryValueBucketNode.VALUES_PER_BUCKET, oldEntryCount - firstId + 1);
      if (oldLeaf.key != 0) {
        final ValueDictionaryValueBucketNode oldBucket =
            valueBucket(oldLeaf.key, bucket, namePage, databaseType, writer);
        if (oldBucket.size() != expectedOldSize) {
          throw new IllegalStateException("reverse bucket disagrees with dictionary cardinality");
        }
        // Completed runs carry over BY REFERENCE. An append never rewrites a closed sub-block, so a
        // bucket that is being extended keeps every block key it already had and only gains new ones.
        for (int i = 0; i < oldBucket.blockCount(); i++) {
          // Skip ONLY the run an extended replacement supersedes. Every other completed block keeps
          // its key, so older revisions continue to address exactly the records they always did.
          if (reversePlan.extendsTail() && oldBucket.blockKey(i) == reversePlan.tailReplacedKey) {
            continue;
          }
          dirBlockFirst.add(oldBucket.blockFirstId(i));
          dirBlockCount.add(oldBucket.blockIdCount(i));
          dirBlockKeys.add(oldBucket.blockKey(i));
        }
        for (int i = 0; i < oldBucket.spillCount(); i++) {
          dirSpillIds.add(oldBucket.spillId(i));
          dirSpillKeys.add(oldBucket.spillKeyAt(i));
        }
      } else if (expectedOldSize != 0) {
        throw new IllegalStateException("missing reverse bucket below dictionary cardinality");
      }
      // New runs from THIS append that fall inside this bucket. Both plan arrays ascend and the
      // bucket loop ascends, so each cursor advances at most once per run across the WHOLE loop —
      // no run is ever examined twice and none is rescanned by a later bucket.
      final long bucketEndExclusive = (long) firstId + size;
      while (planBlockCursor < reversePlan.blockFirstLocal.length) {
        final long absoluteFirst = planBlockCursor == 0 && reversePlan.extendsTail()
            ? reversePlan.tailFirstAbsoluteId
            : (long) oldEntryCount + reversePlan.blockFirstLocal[planBlockCursor];
        if (absoluteFirst >= bucketEndExclusive) {
          break;
        }
        if (absoluteFirst >= firstId) {
          dirBlockFirst.add(Math.toIntExact(absoluteFirst));
          dirBlockCount.add(reversePlan.blockCounts[planBlockCursor]);
          dirBlockKeys.add(reversePlan.blockKeys[planBlockCursor]);
        }
        planBlockCursor++;
      }
      while (planSpillCursor < reversePlan.spillLocals.length) {
        final long absoluteId = (long) oldEntryCount + reversePlan.spillLocals[planSpillCursor];
        if (absoluteId >= bucketEndExclusive) {
          break;
        }
        if (absoluteId >= firstId) {
          dirSpillIds.add(Math.toIntExact(absoluteId));
          dirSpillKeys.add(reversePlan.spillKeys[planSpillCursor]);
        }
        planSpillCursor++;
      }
      final long bucketKey = cursor.next();
      put(ValueDictionaryValueBucketNode.takeOwnership(bucketKey, firstId, size, dirBlockFirst.toArray(),
          dirBlockCount.toArray(), dirBlockKeys.toArray(), dirSpillIds.toArray(), dirSpillKeys.toArray()), namePage,
          databaseType, writer, log);
      if (bucket == lastReverseBucket) {
        break;
      }
    }
    final long reverseRoot = reverseRadixPlan.write(ValueDictionaryRadixNode.REVERSE, oldReverseRoot,
        reverseLeafRunStart, cursor, namePage, databaseType, writer, log);
    cursor.assertExhausted();
    return new Roots(forwardRoot, reverseRoot);
  }

  private static ForwardPlan planForwardUpdate(final long oldForwardRoot, final long oldReverseRoot,
      final int oldEntryCount, final int primaryBucket, final IntList localIds,
      final GlobalValueDictionaryWriter additions, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    final LeafResult oldLeaf = leafKey(oldForwardRoot, ValueDictionaryRadixNode.FORWARD, primaryPath(primaryBucket), 0,
        namePage, databaseType, reader);
    if (oldLeaf.key == 0) {
      if (localIds.size <= MAX_BUCKET_ENTRIES) {
        return ForwardPlan.direct(primaryBucket, Candidates.fromAdditions(localIds, oldEntryCount, additions));
      }
      return ForwardPlan.secondary(primaryBucket, 0L, true, groupBySecondary(localIds, oldEntryCount, additions));
    }
    final DataRecord record = dictionaryRecord(oldLeaf.key, namePage, databaseType, reader);
    if (record instanceof ValueDictionaryHashBucketNode direct) {
      if (direct.getSecondaryDepth() != 0 || direct.getBucket() != primaryBucket
          || direct.size() > MAX_BUCKET_ENTRIES) {
        throw new IllegalStateException("invalid direct value dictionary bucket");
      }
      final Candidates oldCandidates = new Candidates(direct.getHashes(), direct.getIds());
      final Candidates added = Candidates.fromAdditions(localIds, oldEntryCount, additions);
      if ((long) oldCandidates.size() + added.size() <= MAX_BUCKET_ENTRIES) {
        return ForwardPlan.direct(primaryBucket, Candidates.concat(oldCandidates, added));
      }
      final TreeMap<Long, Candidates> groups =
          groupExistingBySecondary(oldCandidates, oldReverseRoot, namePage, databaseType, reader);
      mergeSecondary(groups, groupBySecondary(localIds, oldEntryCount, additions));
      return ForwardPlan.secondary(primaryBucket, 0L, true, groups);
    }
    if (record instanceof ValueDictionaryRadixNode radix && radix.getIndexKind() == ValueDictionaryRadixNode.FORWARD
        && radix.getDepth() == PRIMARY_PATH_BYTES) {
      return ForwardPlan.secondary(primaryBucket, oldLeaf.key, false,
          groupBySecondary(localIds, oldEntryCount, additions));
    }
    throw new IllegalStateException("invalid value dictionary primary leaf");
  }

  static ProbeResult probe(final long forwardRootKey, final long reverseRootKey, final int entryCount,
      final long primaryHash, final long secondaryHash, final byte[] utf8, final int offset, final int length,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    if (entryCount < 0) {
      throw new IllegalArgumentException("entryCount must not be negative");
    }
    final int primaryBucket = hashBucket(primaryHash);
    final LeafResult primary = leafKey(forwardRootKey, ValueDictionaryRadixNode.FORWARD, primaryPath(primaryBucket), 0,
        namePage, databaseType, reader);
    int units = primary.units;
    if (primary.key == 0) {
      return new ProbeResult(GlobalValueDictionary.ID_ABSENT, units);
    }
    final DataRecord primaryRecord = dictionaryRecord(primary.key, namePage, databaseType, reader);
    units++;
    if (primaryRecord instanceof ValueDictionaryHashBucketNode direct) {
      if (direct.getBucket() != primaryBucket || direct.getSecondaryDepth() != 0
          || direct.size() > MAX_BUCKET_ENTRIES) {
        throw new IllegalStateException("invalid direct value dictionary bucket");
      }
      return probeBuckets(direct, reverseRootKey, primaryHash, utf8, offset, length, units, entryCount, namePage,
          databaseType, reader);
    }
    if (!(primaryRecord instanceof ValueDictionaryRadixNode radix)
        || radix.getIndexKind() != ValueDictionaryRadixNode.FORWARD || radix.getDepth() != PRIMARY_PATH_BYTES) {
      throw new IllegalStateException("invalid value dictionary secondary root");
    }
    final int[] secondaryPath = secondaryPath(secondaryHash);
    long bucketKey = radix.childKey(secondaryPath[0]);
    for (int i = 1; i < secondaryPath.length && bucketKey != 0; i++) {
      final ValueDictionaryRadixNode node = radixNode(bucketKey, ValueDictionaryRadixNode.FORWARD,
          PRIMARY_PATH_BYTES + i, namePage, databaseType, reader);
      units++;
      bucketKey = node.childKey(secondaryPath[i]);
    }
    if (bucketKey != 0) {
      final DataRecord leaf = dictionaryRecord(bucketKey, namePage, databaseType, reader);
      if (leaf instanceof ValueDictionaryCollisionNode) {
        return probeCollision(bucketKey, reverseRootKey, utf8, offset, length, units, entryCount, namePage,
            databaseType, reader);
      }
    }
    final TraversalGuard bucketChainGuard =
        new TraversalGuard(bucketKey, MAX_SECONDARY_BUCKET_CHAIN_LENGTH, "secondary bucket chain");
    while (bucketKey != 0) {
      bucketChainGuard.visit(bucketKey);
      final ValueDictionaryHashBucketNode bucket =
          hashBucket(bucketKey, primaryBucket, SECONDARY_PATH_BYTES, secondaryHash, namePage, databaseType, reader);
      units++;
      final ProbeResult result = probeBucket(bucket, reverseRootKey, primaryHash, utf8, offset, length, units,
          entryCount, namePage, databaseType, reader);
      if (result.id != GlobalValueDictionary.ID_ABSENT)
        return result;
      units = result.units;
      bucketKey = bucket.getNextBucketKey();
    }
    return new ProbeResult(GlobalValueDictionary.ID_ABSENT, units);
  }

  private static ProbeResult probeCollision(long key, final long reverseRootKey, final byte[] utf8, final int offset,
      final int length, int units, final int entryCount, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    final TraversalGuard guard = new TraversalGuard(key, MAX_COLLISION_TREE_DEPTH, "collision tree");
    while (key != 0L) {
      guard.visit(key);
      final ValueDictionaryCollisionNode node = collisionNode(key, entryCount, namePage, databaseType, reader);
      units++;
      final EntryResult stored = entryResult(reverseRootKey, node.getId(), entryCount, namePage, databaseType, reader);
      units = Math.addExact(units, stored.units());
      if (stored.entry() == null) {
        return new ProbeResult(GlobalValueDictionary.ID_UNKNOWN, units);
      }
      final int comparison = stored.entry().compareCandidateUnsigned(utf8, offset, length);
      if (comparison == 0) {
        return new ProbeResult(node.getId(), units);
      }
      key = comparison < 0
          ? node.getLeftKey()
          : node.getRightKey();
    }
    return new ProbeResult(GlobalValueDictionary.ID_ABSENT, units);
  }

  private static void prepareCollisionPlans(final ForwardPlan plan, final long oldReverseRoot, final int oldEntryCount,
      final GlobalValueDictionaryWriter additions, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    if (plan.direct != null) {
      return;
    }
    final CollisionPlanningContext context =
        new CollisionPlanningContext(oldReverseRoot, oldEntryCount, additions, namePage, databaseType, reader);
    long records = 0L;
    for (final Map.Entry<Long, Candidates> secondary : plan.secondary.entrySet()) {
      final long oldRoot = plan.rebuildSecondary
          ? 0L
          : leafKey(plan.oldSecondaryRoot, ValueDictionaryRadixNode.FORWARD, secondaryPath(secondary.getKey()),
              PRIMARY_PATH_BYTES, namePage, databaseType, reader).key();
      CollisionTree root = oldRoot == 0L
          ? null
          : new ExistingCollisionTree(oldRoot);
      final Candidates candidates = secondary.getValue();
      final long[] existingPath = new long[MAX_COLLISION_TREE_DEPTH];
      for (int index = 0; index < candidates.size(); index++) {
        final int id = candidates.idAt(index);
        final byte[] exactValue =
            dictionaryValue(id, oldEntryCount, additions, oldReverseRoot, namePage, databaseType, reader);
        root = insertCollisionPlan(root, id, exactValue, context, existingPath, 0);
      }
      records = Math.addExact(records, countPlannedCollisionRecords(root, 0));
      plan.collisionRoots.put(secondary.getKey(), root);
    }
    plan.collisionRecordCount = records;
  }

  private static byte[] dictionaryValue(final int id, final int oldEntryCount,
      final GlobalValueDictionaryWriter additions, final long oldReverseRoot, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineReader reader) {
    if (id <= 0) {
      throw new IllegalArgumentException("dictionary id must be positive");
    }
    if (id <= oldEntryCount) {
      final byte[] value = value(oldReverseRoot, id, namePage, databaseType, reader);
      if (value == null) {
        throw new IllegalStateException("missing value dictionary reverse entry " + id);
      }
      return value;
    }
    return additions.valueBytes(Math.subtractExact(id, oldEntryCount));
  }

  private static CollisionTree insertCollisionPlan(final CollisionTree root, final int id, final byte[] value,
      final CollisionPlanningContext context, final long[] existingPath, final int depth) {
    if (root == null) {
      return new PlannedCollisionTree(id, null, null, 1);
    }
    if (depth >= MAX_COLLISION_TREE_DEPTH) {
      throw new IllegalStateException("value dictionary collision tree exceeds its depth bound");
    }
    if (root instanceof ExistingCollisionTree existing) {
      for (int index = 0; index < depth; index++) {
        if (existingPath[index] == existing.key) {
          throw new IllegalStateException("cycle in value dictionary collision tree");
        }
      }
      existingPath[depth] = existing.key;
      context.validate(existing);
    } else {
      existingPath[depth] = 0L;
    }
    final int rootId = context.id(root);
    final int comparison = context.compareCandidate(value, rootId);
    if (comparison == 0) {
      if (rootId != id) {
        throw new IllegalStateException("duplicate exact value dictionary entry");
      }
      return root;
    }
    final CollisionTree left;
    final CollisionTree right;
    if (comparison < 0) {
      left = insertCollisionPlan(context.left(root), id, value, context, existingPath, depth + 1);
      right = context.right(root);
    } else {
      left = context.left(root);
      right = insertCollisionPlan(context.right(root), id, value, context, existingPath, depth + 1);
    }
    return balanceCollisionPlan(rootId, left, right, context);
  }

  private static CollisionTree balanceCollisionPlan(final int id, final CollisionTree left, final CollisionTree right,
      final CollisionPlanningContext context) {
    final int balance = context.height(left) - context.height(right);
    if (balance > 1) {
      context.validate(left);
      final CollisionTree leftLeft = context.left(left);
      final CollisionTree leftRight = context.right(left);
      if (context.height(leftLeft) < context.height(leftRight)) {
        context.validate(leftRight);
        final CollisionTree rotatedLeft =
            plannedCollision(context.id(left), leftLeft, context.left(leftRight), context);
        final CollisionTree rotatedRight = plannedCollision(id, context.right(leftRight), right, context);
        return plannedCollision(context.id(leftRight), rotatedLeft, rotatedRight, context);
      }
      final CollisionTree rotatedRight = plannedCollision(id, leftRight, right, context);
      return plannedCollision(context.id(left), leftLeft, rotatedRight, context);
    }
    if (balance < -1) {
      context.validate(right);
      final CollisionTree rightLeft = context.left(right);
      final CollisionTree rightRight = context.right(right);
      if (context.height(rightRight) < context.height(rightLeft)) {
        context.validate(rightLeft);
        final CollisionTree rotatedLeft = plannedCollision(id, left, context.left(rightLeft), context);
        final CollisionTree rotatedRight =
            plannedCollision(context.id(right), context.right(rightLeft), rightRight, context);
        return plannedCollision(context.id(rightLeft), rotatedLeft, rotatedRight, context);
      }
      final CollisionTree rotatedLeft = plannedCollision(id, left, rightLeft, context);
      return plannedCollision(context.id(right), rotatedLeft, rightRight, context);
    }
    return plannedCollision(id, left, right, context);
  }

  private static CollisionTree plannedCollision(final int id, final CollisionTree left, final CollisionTree right,
      final CollisionPlanningContext context) {
    final int height = Math.addExact(1, Math.max(context.height(left), context.height(right)));
    if (height > MAX_COLLISION_TREE_DEPTH) {
      throw new IllegalStateException("value dictionary collision tree exceeds its height bound");
    }
    return new PlannedCollisionTree(id, left, right, height);
  }

  private static long countPlannedCollisionRecords(final CollisionTree root, final int depth) {
    if (root == null || root instanceof ExistingCollisionTree) {
      return 0L;
    }
    if (depth >= MAX_COLLISION_TREE_DEPTH) {
      throw new IllegalStateException("planned collision tree exceeds its depth bound");
    }
    final PlannedCollisionTree planned = (PlannedCollisionTree) root;
    return Math.addExact(1L, Math.addExact(countPlannedCollisionRecords(planned.left, depth + 1),
        countPlannedCollisionRecords(planned.right, depth + 1)));
  }

  private static long writeCollisionPlan(final CollisionTree root, final KeyCursor cursor, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineWriter writer, final TransactionIntentLog log,
      final int depth) {
    if (root instanceof ExistingCollisionTree existing) {
      return existing.key;
    }
    if (root == null) {
      return 0L;
    }
    if (depth >= MAX_COLLISION_TREE_DEPTH) {
      throw new IllegalStateException("planned collision tree exceeds its depth bound");
    }
    final PlannedCollisionTree planned = (PlannedCollisionTree) root;
    if (planned.assignedKey != 0L) {
      return planned.assignedKey;
    }
    final long leftKey = writeCollisionPlan(planned.left, cursor, namePage, databaseType, writer, log, depth + 1);
    final long rightKey = writeCollisionPlan(planned.right, cursor, namePage, databaseType, writer, log, depth + 1);
    final long key = cursor.next();
    put(new ValueDictionaryCollisionNode(key, planned.id, planned.height, leftKey, rightKey), namePage, databaseType,
        writer, log);
    planned.assignedKey = key;
    return key;
  }

  private static ProbeResult probeBuckets(final ValueDictionaryHashBucketNode bucket, final long reverseRootKey,
      final long primaryHash, final byte[] utf8, final int offset, final int length, final int units,
      final int entryCount, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    return probeBucket(bucket, reverseRootKey, primaryHash, utf8, offset, length, units, entryCount, namePage,
        databaseType, reader);
  }

  private static ProbeResult probeBucket(final ValueDictionaryHashBucketNode bucket, final long reverseRootKey,
      final long primaryHash, final byte[] utf8, final int offset, final int length, final int initialUnits,
      final int entryCount, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    int units = initialUnits;
    int candidate = lowerBound(bucket, primaryHash);
    while (candidate < bucket.size() && Long.compareUnsigned(bucket.hashAt(candidate), primaryHash) == 0) {
      final int candidateId = bucket.idAt(candidate);
      final EntryResult stored = entryResult(reverseRootKey, candidateId, entryCount, namePage, databaseType, reader);
      units = Math.addExact(units, stored.units);
      if (stored.entry == null) {
        return new ProbeResult(GlobalValueDictionary.ID_UNKNOWN, units);
      }
      if (stored.entry.valueEquals(utf8, offset, length)) {
        return new ProbeResult(candidateId, units);
      }
      candidate++;
    }
    return new ProbeResult(GlobalValueDictionary.ID_ABSENT, units);
  }

  static byte[] value(final long reverseRootKey, final int id, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    return valueResult(reverseRootKey, id, namePage, databaseType, reader).value;
  }

  /**
   * Reverse lookup for a revision-bound {@link GlobalValueDictionary.ReadView}, allocating nothing of
   * its own.
   *
   * <p>
   * The general {@link #entryResult} path exists to report PROBE UNITS for the HFT telemetry the
   * forward probe feeds, and it pays for that with three allocations per call: a three-element
   * {@code int[]} radix path, a {@link LeafResult} and an {@link EntryResult}. The read view discards
   * the unit count entirely, so on a high-cardinality scan — where the view's fixed 256-slot cache
   * cannot hold the working set and nearly every row misses — those three objects were being
   * allocated per row and immediately dropped.
   *
   * <p>
   * This variant walks the same three reverse-radix levels with the path bytes computed inline and
   * the leaf key carried in a local, so the traversal itself allocates nothing. It does NOT remove
   * the cost of materialising a record the reader's own record cache has evicted: that decode is
   * inherent to reading a page-resident record and is bounded by that cache, not by this method.
   *
   * <p>
   * Semantics are those of {@link #entryResult} exactly — same bucket derivation, same
   * {@code maximumId} refusal, same absent/short-bucket answers, same invalid-record failure.
   *
   * @return the entry, or {@code null} when this revision stores no such id
   */
  static ValueDictionaryEntryNode entry(final long reverseRootKey, final int id, final int maximumId,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    if (reverseRootKey == 0 || id <= 0) {
      return null;
    }
    if (id > maximumId) {
      throw new IllegalStateException("value dictionary entry id exceeds header cardinality");
    }
    final int bucket = (id - 1) >>> 8;
    // primaryPath(bucket) inlined: {bucket >>> 16, bucket >>> 8 & 0xFF, bucket & 0xFF}.
    long key = reverseRootKey;
    for (int depth = 0; depth < REVERSE_PATH_BYTES; depth++) {
      if (key == 0) {
        return null;
      }
      final int index = switch (depth) {
        case 0 -> bucket >>> 16;
        case 1 -> bucket >>> 8 & 0xFF;
        default -> bucket & 0xFF;
      };
      key = radixNode(key, ValueDictionaryRadixNode.REVERSE, depth, namePage, databaseType, reader).childKey(index);
    }
    if (key == 0) {
      return null;
    }
    return entryInBucket(valueBucket(key, bucket, namePage, databaseType, reader), id, namePage, databaseType, reader);
  }

  /**
   * The reverse bucket owning {@code id}'s block of 256 ids, or {@code null} when this revision
   * stores none.
   *
   * <p>
   * Exposed so a read view can RETAIN the bucket across probes. A read-only transaction's dictionary
   * record memo is a no-op, so every probe that walks from the root materialises three radix nodes
   * and the bucket before it even reaches the entry — five record decodes per id. One bucket covers
   * 256 consecutive ids, so holding a handful of them collapses that to one decode per id for any
   * scan with locality, without retaining anything per VALUE.
   *
   * @return the bucket node, or {@code null} when the path is absent in this revision
   */
  static ValueDictionaryValueBucketNode valueBucketOf(final long reverseRootKey, final int bucket,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    if (reverseRootKey == 0) {
      return null;
    }
    long key = reverseRootKey;
    for (int depth = 0; depth < REVERSE_PATH_BYTES; depth++) {
      if (key == 0) {
        return null;
      }
      final int index = switch (depth) {
        case 0 -> bucket >>> 16;
        case 1 -> bucket >>> 8 & 0xFF;
        default -> bucket & 0xFF;
      };
      key = radixNode(key, ValueDictionaryRadixNode.REVERSE, depth, namePage, databaseType, reader).childKey(index);
    }
    return key == 0
        ? null
        : valueBucket(key, bucket, namePage, databaseType, reader);
  }

  /**
   * Resolve {@code id} inside an ALREADY resolved reverse bucket — the step a retained bucket lets a
   * read view skip the radix walk for.
   *
   * @return the entry, or {@code null} when the bucket does not cover {@code id}
   */
  static ValueDictionaryEntryNode entryInBucket(final ValueDictionaryValueBucketNode values, final int id,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    if (values == null || id < values.getFirstId() || (long) id >= (long) values.getFirstId() + values.size()) {
      return null;
    }
    final ValueDictionaryEntryNode entry = resolveEntry(values, id, namePage, databaseType, reader);
    return entry;
  }

  private static ValueResult valueResult(final long reverseRootKey, final int id, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineReader reader) {
    final EntryResult result = entryResult(reverseRootKey, id, Integer.MAX_VALUE, namePage, databaseType, reader);
    return new ValueResult(result.entry == null
        ? null
        : result.entry.getValue(), result.units);
  }

  private static EntryResult entryResult(final long reverseRootKey, final int id, final int maximumId,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    if (reverseRootKey == 0 || id <= 0)
      return new EntryResult(null, 0);
    if (id > maximumId) {
      throw new IllegalStateException("value dictionary entry id exceeds header cardinality");
    }
    final int bucket = (id - 1) >>> 8;
    final LeafResult leaf = leafKey(reverseRootKey, ValueDictionaryRadixNode.REVERSE, reversePath(bucket), 0, namePage,
        databaseType, reader);
    if (leaf.key == 0)
      return new EntryResult(null, leaf.units);
    final ValueDictionaryValueBucketNode values = valueBucket(leaf.key, bucket, namePage, databaseType, reader);
    if (id < values.getFirstId() || (long) id >= (long) values.getFirstId() + values.size()) {
      return new EntryResult(null, leaf.units + 1);
    }
    final ValueDictionaryEntryNode entry = resolveEntry(values, id, namePage, databaseType, reader);
    return new EntryResult(entry, leaf.units + 2);
  }

  private static TreeMap<Long, Candidates> groupBySecondary(final IntList localIds, final int oldEntryCount,
      final GlobalValueDictionaryWriter additions) {
    final TreeMap<Long, Candidates> groups = unsignedLongMap();
    for (int i = 0; i < localIds.size; i++) {
      final int localId = localIds.values[i];
      groups.computeIfAbsent(additions.secondaryHashAt(localId), ignored -> new Candidates())
            .add(additions.hashAt(localId), Math.addExact(oldEntryCount, localId));
    }
    return groups;
  }

  private static TreeMap<Long, Candidates> groupExistingBySecondary(final Candidates existing,
      final long reverseRootKey, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    final TreeMap<Long, Candidates> groups = unsignedLongMap();
    for (int i = 0; i < existing.size(); i++) {
      final byte[] value = value(reverseRootKey, existing.idAt(i), namePage, databaseType, reader);
      if (value == null)
        throw new IllegalStateException("missing value dictionary reverse entry");
      final long secondary = GlobalValueDictionary.secondaryValueHash(value, 0, value.length);
      groups.computeIfAbsent(secondary, ignored -> new Candidates()).add(existing.hashAt(i), existing.idAt(i));
    }
    return groups;
  }

  private static void mergeSecondary(final TreeMap<Long, Candidates> target,
      final TreeMap<Long, Candidates> additions) {
    for (final Map.Entry<Long, Candidates> entry : additions.entrySet()) {
      target.merge(entry.getKey(), entry.getValue(), Candidates::concat);
    }
  }

  private static <V> TreeMap<Long, V> unsignedLongMap() {
    return new TreeMap<>(Long::compareUnsigned);
  }

  private static long replaceLeaf(final long oldRoot, final byte indexKind, final int[] path, final int baseDepth,
      final long leafKey, final KeyCursor cursor, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineWriter writer, final TransactionIntentLog log) {
    final ValueDictionaryRadixNode[] oldNodes = new ValueDictionaryRadixNode[path.length];
    long key = oldRoot;
    for (int i = 0; i < path.length && key != 0; i++) {
      final ValueDictionaryRadixNode node = radixNode(key, indexKind, baseDepth + i, namePage, databaseType, writer);
      oldNodes[i] = node;
      key = node.childKey(path[i]);
    }
    long child = leafKey;
    for (int i = path.length - 1; i >= 0; i--) {
      final long nodeKey = cursor.next();
      put(replaceRadixChild(nodeKey, indexKind, (byte) (baseDepth + i), oldNodes[i], path[i], child), namePage,
          databaseType, writer, log);
      child = nodeKey;
    }
    return child;
  }

  private static LeafResult leafKey(final long rootKey, final byte indexKind, final int[] path, final int baseDepth,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    long key = rootKey;
    int units = 0;
    for (int i = 0; i < path.length; i++) {
      if (key == 0)
        return new LeafResult(0, units);
      final ValueDictionaryRadixNode node = radixNode(key, indexKind, baseDepth + i, namePage, databaseType, reader);
      units++;
      key = node.childKey(path[i]);
    }
    return new LeafResult(key, units);
  }

  private static ValueDictionaryRadixNode radixNode(final long key, final byte indexKind, final int depth,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    final DataRecord record = dictionaryRecord(key, namePage, databaseType, reader);
    if (!(record instanceof ValueDictionaryRadixNode node) || node.getIndexKind() != indexKind
        || node.getDepth() != depth) {
      throw new IllegalStateException("invalid value dictionary radix path");
    }
    return node;
  }

  private static ValueDictionaryHashBucketNode hashBucket(final long key, final int primaryBucket,
      final int secondaryDepth, final long secondaryPrefix, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    final DataRecord record = dictionaryRecord(key, namePage, databaseType, reader);
    if (!(record instanceof ValueDictionaryHashBucketNode node) || node.getBucket() != primaryBucket
        || node.getSecondaryDepth() != secondaryDepth || node.getSecondaryPrefix() != secondaryPrefix
        || node.size() > MAX_BUCKET_ENTRIES) {
      throw new IllegalStateException("invalid value dictionary forward bucket");
    }
    return node;
  }

  /**
   * Resolve one id through its bucket's sparse directory: either it is packed in a sub-block, or it
   * spilled to its own record.
   *
   * <p>
   * NOTE: for a PACKED id this still materialises an entry node over a copied slice. That is a
   * correctness bridge, not the end state — {@link GlobalValueDictionary.ReadView} is what must
   * ultimately hold the block's backing array, offset and length and compare in place. Until it does,
   * no claim of per-id allocation freedom is made for packed ids.
   */
  private static ValueDictionaryEntryNode resolveEntry(final ValueDictionaryValueBucketNode values, final int id,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    final long blockKey = values.blockKeyCovering(id);
    if (blockKey != 0L) {
      final ValueDictionaryValueBlockNode block = blockNode(blockKey, id, namePage, databaseType, reader);
      final int offset = block.valueOffset(id);
      final int length = block.valueLength(id);
      final byte[] value = java.util.Arrays.copyOfRange(block.rawBytes(), offset, offset + length);
      return ValueDictionaryEntryNode.takeOwnership(blockKey, value);
    }
    final long spillKey = values.spillKeyCovering(id);
    if (spillKey == 0L) {
      throw new IllegalStateException("value dictionary bucket covers neither a block nor a spill for id " + id);
    }
    final DataRecord record = dictionaryRecord(spillKey, namePage, databaseType, reader);
    if (!(record instanceof ValueDictionaryEntryNode entry)) {
      throw new IllegalStateException("invalid value dictionary entry");
    }
    return entry;
  }

  /**
   * Decode one packed sub-block and check it really covers {@code id}. Exposed so a read view can
   * RETAIN decoded blocks: a block is up to 64 KiB, so decoding it per probe is by far the largest
   * term in a high-cardinality miss.
   */
  static ValueDictionaryValueBlockNode blockNode(final long blockKey, final int id, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineReader reader) {
    final DataRecord record = dictionaryRecord(blockKey, namePage, databaseType, reader);
    if (!(record instanceof ValueDictionaryValueBlockNode block) || !block.covers(id)) {
      throw new IllegalStateException("invalid value dictionary sub-block for id " + id);
    }
    return block;
  }

  /** The entry record of a SPILLED id. */
  static ValueDictionaryEntryNode spillEntry(final long spillKey, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineReader reader) {
    final DataRecord record = dictionaryRecord(spillKey, namePage, databaseType, reader);
    if (!(record instanceof ValueDictionaryEntryNode entry)) {
      throw new IllegalStateException("invalid value dictionary entry");
    }
    return entry;
  }

  private static ValueDictionaryValueBucketNode valueBucket(final long key, final int bucket, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineReader reader) {
    final DataRecord record = dictionaryRecord(key, namePage, databaseType, reader);
    if (!(record instanceof ValueDictionaryValueBucketNode node) || ((node.getFirstId() - 1) >>> 8) != bucket) {
      throw new IllegalStateException("invalid value dictionary reverse bucket");
    }
    return node;
  }

  private static DataRecord dictionaryRecord(final long key, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    final DataRecord record = namePage.getProjectionValueDictionaryRecord(key, databaseType, reader);
    if (record == null)
      throw new IllegalStateException("missing value dictionary record " + key);
    return record;
  }

  private static ValueDictionaryCollisionNode collisionNode(final long key, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineReader reader) {
    return collisionNode(key, Integer.MAX_VALUE, namePage, databaseType, reader);
  }

  private static ValueDictionaryCollisionNode collisionNode(final long key, final int maximumId,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    final DataRecord record = dictionaryRecord(key, namePage, databaseType, reader);
    if (!(record instanceof ValueDictionaryCollisionNode node)) {
      throw new IllegalStateException("invalid value dictionary collision tree");
    }
    if (node.getId() > maximumId || node.getHeight() > MAX_COLLISION_TREE_DEPTH || node.getLeftKey() == key
        || node.getRightKey() == key) {
      throw new IllegalStateException("corrupt value dictionary collision tree");
    }
    return node;
  }

  private static void put(final DataRecord record, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineWriter writer, final TransactionIntentLog log) {
    namePage.putProjectionValueDictionaryRecord(record, databaseType, writer, log);
  }

  private static int recordBytes(final DataRecord record) {
    if (record instanceof ValueDictionaryEntryNode entry) {
      return Integer.BYTES + entry.getValueLength();
    }
    if (record instanceof ValueDictionaryRadixNode) {
      final ValueDictionaryRadixNode radix = (ValueDictionaryRadixNode) record;
      return 2 + Short.BYTES + radix.childCount() * (Byte.BYTES + Long.BYTES);
    }
    if (record instanceof ValueDictionaryHashBucketNode bucket) {
      return 25 + bucket.size() * (Long.BYTES + Integer.BYTES);
    }
    if (record instanceof ValueDictionaryValueBucketNode bucket) {
      return 2 * Integer.BYTES + bucket.size() * Long.BYTES;
    }
    if (record instanceof ValueDictionaryCollisionNode) {
      return 2 * Integer.BYTES + 2 * Long.BYTES;
    }
    throw new IllegalArgumentException("unsupported value dictionary record " + record.getKind());
  }

  private static int hashBucket(final long hash) {
    return (int) (hash >>> 40) & BUCKET_MASK;
  }

  private static int[] primaryPath(final int bucket) {
    return new int[] {bucket >>> 16, bucket >>> 8 & 0xFF, bucket & 0xFF};
  }

  private static int[] secondaryPath(final long hash) {
    final int[] path = new int[SECONDARY_PATH_BYTES];
    for (int i = 0; i < SECONDARY_PATH_BYTES; i++) {
      path[i] = (int) (hash >>> (56 - i * 8)) & 0xFF;
    }
    return path;
  }

  private static int[] reversePath(final int bucket) {
    return primaryPath(bucket);
  }

  private static int lowerBound(final ValueDictionaryHashBucketNode bucket, final long wanted) {
    int low = 0;
    int high = bucket.size();
    while (low < high) {
      final int middle = (low + high) >>> 1;
      if (Long.compareUnsigned(bucket.hashAt(middle), wanted) < 0)
        low = middle + 1;
      else
        high = middle;
    }
    return low;
  }

  private static int compareUnsigned(final byte[] left, final int leftOffset, final int leftLength,
      final byte[] right) {
    final int common = Math.min(leftLength, right.length);
    for (int index = 0; index < common; index++) {
      final int comparison =
          Integer.compare(Byte.toUnsignedInt(left[leftOffset + index]), Byte.toUnsignedInt(right[index]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(leftLength, right.length);
  }

  private static void sortPairs(final long[] hashes, final int[] ids, final int low, final int high) {
    if (low >= high)
      return;
    int left = low;
    int right = high;
    final long pivot = hashes[(low + high) >>> 1];
    while (left <= right) {
      while (Long.compareUnsigned(hashes[left], pivot) < 0)
        left++;
      while (Long.compareUnsigned(hashes[right], pivot) > 0)
        right--;
      if (left <= right) {
        final long hash = hashes[left];
        hashes[left] = hashes[right];
        hashes[right] = hash;
        final int id = ids[left];
        ids[left] = ids[right];
        ids[right] = id;
        left++;
        right--;
      }
    }
    if (low < right)
      sortPairs(hashes, ids, low, right);
    if (left < high)
      sortPairs(hashes, ids, left, high);
  }

  private static long estimateWorkspaceBytes(final long recordCount, final TreeMap<Integer, IntList> primaryGroups,
      final RadixPlan forwardRadixPlan, final DenseRadixPlan reverseRadixPlan,
      final GlobalValueDictionaryWriter additions) {
    long radixNodes = Math.addExact(forwardRadixPlan.nodeCount(), reverseRadixPlan.nodeCount());
    long hashBucketBytes = 0L;
    for (final IntList primaryGroup : primaryGroups.values()) {
      final ForwardPlan plan = primaryGroup.forwardPlan;
      if (plan.direct != null) {
        hashBucketBytes =
            Math.addExact(hashBucketBytes, 25L + (long) plan.direct.size() * (Long.BYTES + Integer.BYTES));
      } else {
        for (int ignored = 0; ignored < plan.secondary.size(); ignored++) {
          radixNodes = Math.addExact(radixNodes, SECONDARY_PATH_BYTES);
        }
        hashBucketBytes = Math.addExact(hashBucketBytes,
            Math.multiplyExact(plan.collisionRecordCount, 2L * Integer.BYTES + 2L * Long.BYTES));
      }
    }
    final long radixBytes = Math.multiplyExact(radixNodes, 2L + Short.BYTES + 16L * (Byte.BYTES + Long.BYTES));
    final long reverseBytes = Math.addExact(Math.multiplyExact((long) additions.entryCount(), Long.BYTES),
        Math.multiplyExact(reverseRadixPlan.leafCount(), 2L * Integer.BYTES));
    final long entryBytes =
        Math.addExact(additions.valueBytes(), Math.multiplyExact((long) additions.entryCount(), Integer.BYTES));
    final long encodedBytes =
        Math.addExact(entryBytes, Math.addExact(radixBytes, Math.addExact(hashBucketBytes, reverseBytes)));
    final long pageAndTilBytes =
        Math.multiplyExact(2L, Math.addExact(encodedBytes, Math.multiplyExact(recordCount, RECORD_STRIDE)));
    return Math.addExact(pageAndTilBytes, Math.multiplyExact((long) additions.entryCount(), 64L));
  }

  /**
   * The ONE decision about how an append's values are laid out: which consecutive runs are packed
   * into sub-blocks and which individual ids spill to their own record.
   *
   * <p>
   * Built once, then consumed by BOTH the exact record-count arithmetic and the emit loop. Deriving
   * it twice is how the count and the emission drift apart, and while {@code KeyCursor} would catch
   * that loudly, catching it is worse than not being able to express it.
   */
  private static final class ReverseAppendPlan {
    private final int[] blockFirstLocal;
    private final int[] blockCounts;
    private final long[] blockKeys;
    private final int[] spillLocals;
    private final long[] spillKeys;
    /** Old tail block replaced by an extended copy, or {@code 0} when none was extendable. */
    private final long tailReplacedKey;
    /** Absolute first id the extended block keeps — the OLD tail's, not the first addition's. */
    private final int tailFirstAbsoluteId;
    /** The old tail's values, carried into the replacement. */
    private final int[] tailPrefixOffsets;
    private final byte[] tailPrefixBytes;

    private ReverseAppendPlan(final int[] blockFirstLocal, final int[] blockCounts, final int[] spillLocals,
        final long tailReplacedKey, final int tailFirstAbsoluteId, final int[] tailPrefixOffsets,
        final byte[] tailPrefixBytes) {
      this.blockFirstLocal = blockFirstLocal;
      this.blockCounts = blockCounts;
      this.blockKeys = new long[blockFirstLocal.length];
      this.spillLocals = spillLocals;
      this.spillKeys = new long[spillLocals.length];
      this.tailReplacedKey = tailReplacedKey;
      this.tailFirstAbsoluteId = tailFirstAbsoluteId;
      this.tailPrefixOffsets = tailPrefixOffsets;
      this.tailPrefixBytes = tailPrefixBytes;
    }

    private boolean extendsTail() {
      return tailReplacedKey != 0L;
    }

    /** Values the old tail contributes to the replacement block. */
    private int tailPrefixCount() {
      return tailPrefixOffsets == null
          ? 0
          : tailPrefixOffsets.length - 1;
    }

    private int recordCount() {
      return blockFirstLocal.length + spillLocals.length;
    }
  }

  /**
   * The old last packed run, when it can absorb more values.
   *
   * <p>
   * Only the OLD TAIL is ever considered. Every completed block is immutable and keeps its key, so a
   * revision that adds one value must not rewrite the bucket's history — it copies at most the one
   * run that was still open and leaves the rest addressed exactly as before. Without this, each
   * one-value revision opened a fresh one-value block and the directory grew a run per revision.
   */
  private record OpenTail(long key, int firstId, int[] offsets, byte[] bytes) {
  }

  private static OpenTail findExtendableTail(final long oldReverseRoot, final int oldEntryCount,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineWriter writer) {
    if (oldReverseRoot == 0L || oldEntryCount <= 0) {
      return null;
    }
    // The next id must land in the SAME 256-id bucket, or the run would have to span a directory it
    // does not belong to.
    if (((oldEntryCount - 1) >>> 8) != (oldEntryCount >>> 8)) {
      return null;
    }
    final ValueDictionaryValueBucketNode bucket =
        valueBucketOf(oldReverseRoot, (oldEntryCount - 1) >>> 8, namePage, databaseType, writer);
    if (bucket == null) {
      return null;
    }
    final long key = bucket.blockKeyCovering(oldEntryCount);
    if (key == 0L) {
      return null; // the last id SPILLED — nothing open to extend
    }
    final ValueDictionaryValueBlockNode block = blockNode(key, oldEntryCount, namePage, databaseType, writer);
    // It must END exactly at the old cardinality; a block covering ids beyond it is not the tail.
    if ((long) block.getFirstId() + block.size() - 1L != oldEntryCount) {
      return null;
    }
    if (block.size() >= ValueDictionaryValueBlockNode.MAX_BLOCK_VALUES
        || block.rawBytes().length >= ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES) {
      return null; // full — start a new block
    }
    return new OpenTail(key, block.getFirstId(), block.copyOffsets(), block.rawBytes());
  }

  /**
   * Lay out an append: consecutive values pack into byte-bounded sub-blocks, and ONLY a value longer
   * than a whole block spills to its own record. A block also closes at a bucket boundary, because
   * the directory that addresses it lives in the bucket.
   */
  private static ReverseAppendPlan planReverseAppend(final GlobalValueDictionaryWriter additions,
      final int oldEntryCount, final OpenTail tail) {
    final IntList blockFirst = new IntList();
    final IntList blockCount = new IntList();
    final IntList spills = new IntList();
    int openFirst = 0;
    int openCount = 0;
    long openBytes = 0;
    // Seed the first open block from the old tail so the leading additions extend IT rather than
    // opening a run of their own. The seeded count and bytes make the same capacity tests below
    // decide when the extended block must close, so the tail cannot be overfilled.
    boolean extending = false;
    if (tail != null) {
      openFirst = 1;
      openCount = tail.offsets().length - 1;
      openBytes = tail.bytes().length;
      extending = true;
    }
    for (int localId = 1; localId <= additions.entryCount(); localId++) {
      final int length = additions.valueBytes(localId).length;
      final int absoluteId = Math.addExact(oldEntryCount, localId);
      if (length > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES) {
        if (openCount > 0) {
          blockFirst.add(openFirst);
          blockCount.add(openCount);
          openCount = 0;
          openBytes = 0;
        } else if (extending && blockFirst.size == 0) {
          // The very first addition spills, so the seeded tail never grew: leave it alone.
          extending = false;
        }
        spills.add(localId);
        continue;
      }
      final int openFirstAbsolute = extending && blockFirst.size == 0
          ? tail.firstId()
          : Math.addExact(oldEntryCount, openFirst);
      final boolean crossesBucket = openCount > 0 && ((absoluteId - 1) >>> 8) != ((openFirstAbsolute - 1) >>> 8);
      if (openCount > 0 && (crossesBucket || openCount == ValueDictionaryValueBlockNode.MAX_BLOCK_VALUES
          || openBytes + length > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES)) {
        blockFirst.add(openFirst);
        blockCount.add(openCount);
        openCount = 0;
        openBytes = 0;
      }
      if (openCount == 0) {
        openFirst = localId;
      }
      openCount++;
      openBytes += length;
    }
    if (openCount > 0) {
      blockFirst.add(openFirst);
      blockCount.add(openCount);
    } else if (extending && blockFirst.size == 0) {
      extending = false;
    }
    // The extended block is block 0 and its count already includes the tail's values; every other
    // planned block counts only its own additions.
    return extending
        ? new ReverseAppendPlan(blockFirst.toArray(), blockCount.toArray(), spills.toArray(), tail.key(),
            tail.firstId(), tail.offsets(), tail.bytes())
        : new ReverseAppendPlan(blockFirst.toArray(), blockCount.toArray(), spills.toArray(), 0L, 0, null, null);
  }

  private record LeafResult(long key, int units) {
  }

  private record ValueResult(byte[] value, int units) {
  }

  private record EntryResult(ValueDictionaryEntryNode entry, int units) {
  }

  private interface CollisionTree {
  }

  private static final class ExistingCollisionTree implements CollisionTree {
    private final long key;
    private ValueDictionaryCollisionNode node;
    private CollisionTree left;
    private CollisionTree right;
    private boolean childrenLoaded;
    private boolean validated;

    private ExistingCollisionTree(final long key) {
      if (key <= 0L) {
        throw new IllegalArgumentException("existing collision key must be positive");
      }
      this.key = key;
    }
  }

  private static final class PlannedCollisionTree implements CollisionTree {
    private final int id;
    private final CollisionTree left;
    private final CollisionTree right;
    private final int height;
    private long assignedKey;

    private PlannedCollisionTree(final int id, final CollisionTree left, final CollisionTree right, final int height) {
      this.id = id;
      this.left = left;
      this.right = right;
      this.height = height;
    }
  }

  private static final class CollisionPlanningContext {
    private final long oldReverseRoot;
    private final int oldEntryCount;
    private final int finalEntryCount;
    private final GlobalValueDictionaryWriter additions;
    private final NamePage namePage;
    private final DatabaseType databaseType;
    private final StorageEngineReader reader;

    private CollisionPlanningContext(final long oldReverseRoot, final int oldEntryCount,
        final GlobalValueDictionaryWriter additions, final NamePage namePage, final DatabaseType databaseType,
        final StorageEngineReader reader) {
      this.oldReverseRoot = oldReverseRoot;
      this.oldEntryCount = oldEntryCount;
      finalEntryCount = Math.addExact(oldEntryCount, additions.entryCount());
      this.additions = additions;
      this.namePage = namePage;
      this.databaseType = databaseType;
      this.reader = reader;
    }

    private int id(final CollisionTree tree) {
      if (tree instanceof PlannedCollisionTree planned) {
        validateId(planned.id);
        return planned.id;
      }
      final ExistingCollisionTree existing = (ExistingCollisionTree) tree;
      return node(existing).getId();
    }

    private int height(final CollisionTree tree) {
      if (tree == null) {
        return 0;
      }
      return tree instanceof PlannedCollisionTree planned
          ? planned.height
          : node((ExistingCollisionTree) tree).getHeight();
    }

    private CollisionTree left(final CollisionTree tree) {
      if (tree instanceof PlannedCollisionTree planned) {
        return planned.left;
      }
      final ExistingCollisionTree existing = (ExistingCollisionTree) tree;
      loadChildren(existing);
      return existing.left;
    }

    private CollisionTree right(final CollisionTree tree) {
      if (tree instanceof PlannedCollisionTree planned) {
        return planned.right;
      }
      final ExistingCollisionTree existing = (ExistingCollisionTree) tree;
      loadChildren(existing);
      return existing.right;
    }

    private int compareCandidate(final byte[] candidate, final int storedId) {
      validateId(storedId);
      if (storedId <= oldEntryCount) {
        final EntryResult stored = entryResult(oldReverseRoot, storedId, oldEntryCount, namePage, databaseType, reader);
        if (stored.entry == null) {
          throw new IllegalStateException("missing value dictionary reverse entry " + storedId);
        }
        return stored.entry.compareCandidateUnsigned(candidate, 0, candidate.length);
      }
      return additions.compareCandidateUnsigned(candidate, 0, candidate.length,
          Math.subtractExact(storedId, oldEntryCount));
    }

    private void validate(final CollisionTree tree) {
      if (!(tree instanceof ExistingCollisionTree existing) || existing.validated) {
        return;
      }
      loadChildren(existing);
      final ValueDictionaryCollisionNode node = node(existing);
      final int leftHeight = height(existing.left);
      final int rightHeight = height(existing.right);
      if (node.getHeight() != Math.addExact(1, Math.max(leftHeight, rightHeight))
          || Math.abs(leftHeight - rightHeight) > 1) {
        throw new IllegalStateException("corrupt value dictionary collision tree shape");
      }
      existing.validated = true;
    }

    private ValueDictionaryCollisionNode node(final ExistingCollisionTree existing) {
      if (existing.node == null) {
        existing.node = collisionNode(existing.key, oldEntryCount, namePage, databaseType, reader);
      }
      return existing.node;
    }

    private void loadChildren(final ExistingCollisionTree existing) {
      if (existing.childrenLoaded) {
        return;
      }
      final ValueDictionaryCollisionNode node = node(existing);
      existing.left = node.getLeftKey() == 0L
          ? null
          : new ExistingCollisionTree(node.getLeftKey());
      existing.right = node.getRightKey() == 0L
          ? null
          : new ExistingCollisionTree(node.getRightKey());
      existing.childrenLoaded = true;
    }

    private void validateId(final int id) {
      if (id <= 0 || id > finalEntryCount) {
        throw new IllegalStateException("collision id exceeds value dictionary cardinality");
      }
    }
  }

  /** Allocation-free Brent cycle detector with a hard visit bound. */
  private static final class TraversalGuard {
    private final int maximumVisits;
    private final String structure;
    private long checkpoint;
    private int checkpointPeriod = 1;
    private int sinceCheckpoint;
    private int visits;

    private TraversalGuard(final long firstKey, final int maximumVisits, final String structure) {
      this.maximumVisits = maximumVisits;
      this.structure = structure;
      checkpoint = firstKey;
    }

    private void visit(final long key) {
      if (key <= 0) {
        throw new IllegalStateException("invalid " + structure + " key");
      }
      if (++visits > maximumVisits) {
        throw new IllegalStateException(structure + " exceeds its traversal bound");
      }
      if (visits == 1) {
        return;
      }
      if (key == checkpoint) {
        throw new IllegalStateException("cycle in value dictionary " + structure);
      }
      if (++sinceCheckpoint == checkpointPeriod) {
        checkpoint = key;
        checkpointPeriod = Math.min(maximumVisits, checkpointPeriod << 1);
        sinceCheckpoint = 0;
      }
    }
  }

  private static final class ForwardPlan {
    private final int primaryBucket;
    private final Candidates direct;
    private final long oldSecondaryRoot;
    private final boolean rebuildSecondary;
    private final TreeMap<Long, Candidates> secondary;
    private final TreeMap<Long, CollisionTree> collisionRoots;
    private long collisionRecordCount;

    private ForwardPlan(final int primaryBucket, final Candidates direct, final long oldSecondaryRoot,
        final boolean rebuildSecondary, final TreeMap<Long, Candidates> secondary) {
      this.primaryBucket = primaryBucket;
      this.direct = direct;
      this.oldSecondaryRoot = oldSecondaryRoot;
      this.rebuildSecondary = rebuildSecondary;
      this.secondary = secondary;
      collisionRoots = secondary == null
          ? null
          : unsignedLongMap();
    }

    private static ForwardPlan direct(final int primaryBucket, final Candidates candidates) {
      return new ForwardPlan(primaryBucket, candidates, 0L, false, null);
    }

    private static ForwardPlan secondary(final int primaryBucket, final long oldSecondaryRoot, final boolean rebuild,
        final TreeMap<Long, Candidates> groups) {
      return new ForwardPlan(primaryBucket, null, oldSecondaryRoot, rebuild, groups);
    }

    private long newRecordCount() {
      if (direct != null)
        return 1L;
      long count = 0L;
      for (int ignored = 0; ignored < secondary.size(); ignored++) {
        count = Math.addExact(count, SECONDARY_PATH_BYTES);
      }
      return count;
    }
  }

  private static final class RadixPlan {
    private static final int LEVEL_TWO_PREFIXES = 1 << 16;
    private static final int LEVEL_ONE_PREFIXES = 1 << 8;

    private final PrefixList levelTwo = new PrefixList(LEVEL_TWO_PREFIXES);
    private final PrefixList levelOne = new PrefixList(LEVEL_ONE_PREFIXES);

    private static RadixPlan forBuckets(final Iterable<Integer> buckets) {
      final RadixPlan plan = new RadixPlan();
      for (final int bucket : buckets) {
        plan.levelTwo.addSorted(bucket >>> 8);
        plan.levelOne.addSorted(bucket >>> 16);
      }
      return plan;
    }

    private long nodeCount() {
      return levelTwo.size == 0
          ? 0L
          : (long) levelTwo.size + levelOne.size + 1L;
    }

    private long write(final byte indexKind, final long oldRoot, final TreeMap<Integer, IntList> primaryGroups,
        final KeyCursor cursor, final NamePage namePage, final DatabaseType databaseType,
        final StorageEngineWriter writer, final TransactionIntentLog log) {
      if (primaryGroups.isEmpty())
        return oldRoot;
      final ValueDictionaryRadixNode oldRootNode = oldRoot == 0L
          ? null
          : radixNode(oldRoot, indexKind, 0, namePage, databaseType, writer);
      final long levelTwoRunStart = cursor.peek();
      int cachedHigh = -1;
      ValueDictionaryRadixNode oldLevelOne = null;
      for (int index = 0; index < levelTwo.size; index++) {
        final int prefix = levelTwo.values[index];
        final int high = prefix >>> 8;
        final int middle = prefix & 0xFF;
        if (high != cachedHigh) {
          cachedHigh = high;
          final long oldLevelOneKey = oldRootNode == null
              ? 0L
              : oldRootNode.childKey(high);
          oldLevelOne = oldLevelOneKey == 0L
              ? null
              : radixNode(oldLevelOneKey, indexKind, 1, namePage, databaseType, writer);
        }
        final long oldLevelTwoKey = oldLevelOne == null
            ? 0L
            : oldLevelOne.childKey(middle);
        ValueDictionaryRadixNode updated = oldLevelTwoKey == 0
            ? null
            : radixNode(oldLevelTwoKey, indexKind, 2, namePage, databaseType, writer);
        final int firstBucket = prefix << 8;
        final int lastBucket = firstBucket | 0xFF;
        for (final Map.Entry<Integer, IntList> leaf : primaryGroups.subMap(firstBucket, true, lastBucket, true)
                                                                   .entrySet()) {
          final long leafKey = leaf.getValue().writtenLeafKey;
          if (leafKey <= 0L) {
            throw new IllegalStateException("forward radix leaf was not written");
          }
          updated = replaceRadixChild(1L, indexKind, (byte) 2, updated, leaf.getKey() & 0xFF, leafKey);
        }
        final long nodeKey = cursor.next();
        put(copyRadixNode(nodeKey, updated), namePage, databaseType, writer, log);
      }
      final long levelOneRunStart = cursor.peek();
      int levelTwoIndex = 0;
      for (int index = 0; index < levelOne.size; index++) {
        final int high = levelOne.values[index];
        final long oldLevelOneKey = oldRootNode == null
            ? 0L
            : oldRootNode.childKey(high);
        ValueDictionaryRadixNode updated = oldLevelOneKey == 0
            ? null
            : radixNode(oldLevelOneKey, indexKind, 1, namePage, databaseType, writer);
        while (levelTwoIndex < levelTwo.size && (levelTwo.values[levelTwoIndex] >>> 8) == high) {
          final int prefix = levelTwo.values[levelTwoIndex];
          updated = replaceRadixChild(1L, indexKind, (byte) 1, updated, prefix & 0xFF,
              recordKeyAt(levelTwoRunStart, levelTwoIndex));
          levelTwoIndex++;
        }
        final long nodeKey = cursor.next();
        put(copyRadixNode(nodeKey, updated), namePage, databaseType, writer, log);
      }
      if (levelTwoIndex != levelTwo.size) {
        throw new IllegalStateException("forward radix prefix plan was not consumed exactly");
      }
      ValueDictionaryRadixNode updatedRoot = oldRoot == 0
          ? null
          : oldRootNode;
      int levelOneIndex = 0;
      for (int index = 0; index < levelOne.size; index++) {
        final int high = levelOne.values[index];
        updatedRoot =
            replaceRadixChild(1L, indexKind, (byte) 0, updatedRoot, high, recordKeyAt(levelOneRunStart, levelOneIndex));
        levelOneIndex++;
      }
      final long rootKey = cursor.next();
      put(copyRadixNode(rootKey, updatedRoot), namePage, databaseType, writer, log);
      return rootKey;
    }

    /** Sorted primitive prefix set; its largest backing payload is 256 KiB plus one array header. */
    private static final class PrefixList {
      private final int maximumSize;
      private int[] values = new int[4];
      private int size;

      private PrefixList(final int maximumSize) {
        this.maximumSize = maximumSize;
      }

      private void addSorted(final int value) {
        if (value < 0 || value >= maximumSize || size > 0 && value < values[size - 1]) {
          throw new IllegalArgumentException("invalid ordered radix prefix");
        }
        if (size > 0 && value == values[size - 1]) {
          return;
        }
        if (size == values.length) {
          values = Arrays.copyOf(values, Math.min(maximumSize, size << 1));
        }
        values[size++] = value;
      }
    }
  }

  /**
   * Reverse ids occupy one dense interval, so their buckets and both radix-prefix levels do too.
   * Representing those ranges as four integers avoids a {@code TreeSet<Integer>} node per bucket and
   * lets every child key be derived from the exact reserved run.
   */
  private static final class DenseRadixPlan {
    private final int firstBucket;
    private final int lastBucket;
    private final int firstLevelTwo;
    private final int lastLevelTwo;
    private final int firstLevelOne;
    private final int lastLevelOne;

    private DenseRadixPlan(final int firstBucket, final int lastBucket) {
      if (firstBucket < 0 || firstBucket > lastBucket || lastBucket > BUCKET_MASK) {
        throw new IllegalArgumentException("invalid dense value dictionary radix range");
      }
      this.firstBucket = firstBucket;
      this.lastBucket = lastBucket;
      firstLevelTwo = firstBucket >>> 8;
      lastLevelTwo = lastBucket >>> 8;
      firstLevelOne = firstBucket >>> 16;
      lastLevelOne = lastBucket >>> 16;
    }

    private long leafCount() {
      return (long) lastBucket - firstBucket + 1L;
    }

    private long nodeCount() {
      final long levelTwoCount = (long) lastLevelTwo - firstLevelTwo + 1L;
      final long levelOneCount = (long) lastLevelOne - firstLevelOne + 1L;
      return Math.addExact(Math.addExact(levelTwoCount, levelOneCount), 1L);
    }

    private long write(final byte indexKind, final long oldRoot, final long firstLeafKey, final KeyCursor cursor,
        final NamePage namePage, final DatabaseType databaseType, final StorageEngineWriter writer,
        final TransactionIntentLog log) {
      if (firstLeafKey <= 0L) {
        throw new IllegalArgumentException("dense radix leaves must have positive keys");
      }
      final ValueDictionaryRadixNode oldRootNode = oldRoot == 0L
          ? null
          : radixNode(oldRoot, indexKind, 0, namePage, databaseType, writer);

      final long levelTwoRunStart = cursor.peek();
      int cachedHigh = -1;
      ValueDictionaryRadixNode oldLevelOne = null;
      for (int prefix = firstLevelTwo;; prefix++) {
        final int high = prefix >>> 8;
        if (high != cachedHigh) {
          cachedHigh = high;
          final long oldLevelOneKey = oldRootNode == null
              ? 0L
              : oldRootNode.childKey(high);
          oldLevelOne = oldLevelOneKey == 0L
              ? null
              : radixNode(oldLevelOneKey, indexKind, 1, namePage, databaseType, writer);
        }
        final long oldLevelTwoKey = oldLevelOne == null
            ? 0L
            : oldLevelOne.childKey(prefix & 0xFF);
        final ValueDictionaryRadixNode oldLevelTwo = oldLevelTwoKey == 0L
            ? null
            : radixNode(oldLevelTwoKey, indexKind, 2, namePage, databaseType, writer);
        final long[] children = oldLevelTwo == null
            ? new long[ValueDictionaryRadixNode.FANOUT]
            : oldLevelTwo.getChildKeys();
        final int first = Math.max(firstBucket, prefix << 8);
        final int last = Math.min(lastBucket, (prefix << 8) | 0xFF);
        for (int bucket = first;; bucket++) {
          children[bucket & 0xFF] = recordKeyAt(firstLeafKey, (long) bucket - firstBucket);
          if (bucket == last) {
            break;
          }
        }
        final long nodeKey = cursor.next();
        put(new ValueDictionaryRadixNode(nodeKey, indexKind, (byte) 2, children), namePage, databaseType, writer, log);
        if (prefix == lastLevelTwo) {
          break;
        }
      }

      final long levelOneRunStart = cursor.peek();
      for (int high = firstLevelOne;; high++) {
        final long oldLevelOneKey = oldRootNode == null
            ? 0L
            : oldRootNode.childKey(high);
        final ValueDictionaryRadixNode prior = oldLevelOneKey == 0L
            ? null
            : radixNode(oldLevelOneKey, indexKind, 1, namePage, databaseType, writer);
        final long[] children = prior == null
            ? new long[ValueDictionaryRadixNode.FANOUT]
            : prior.getChildKeys();
        final int first = Math.max(firstLevelTwo, high << 8);
        final int last = Math.min(lastLevelTwo, (high << 8) | 0xFF);
        for (int prefix = first;; prefix++) {
          children[prefix & 0xFF] = recordKeyAt(levelTwoRunStart, (long) prefix - firstLevelTwo);
          if (prefix == last) {
            break;
          }
        }
        final long nodeKey = cursor.next();
        put(new ValueDictionaryRadixNode(nodeKey, indexKind, (byte) 1, children), namePage, databaseType, writer, log);
        if (high == lastLevelOne) {
          break;
        }
      }

      final long[] rootChildren = oldRootNode == null
          ? new long[ValueDictionaryRadixNode.FANOUT]
          : oldRootNode.getChildKeys();
      for (int high = firstLevelOne;; high++) {
        rootChildren[high] = recordKeyAt(levelOneRunStart, (long) high - firstLevelOne);
        if (high == lastLevelOne) {
          break;
        }
      }
      final long rootKey = cursor.next();
      put(new ValueDictionaryRadixNode(rootKey, indexKind, (byte) 0, rootChildren), namePage, databaseType, writer,
          log);
      return rootKey;
    }
  }

  private static ValueDictionaryRadixNode replaceRadixChild(final long nodeKey, final byte indexKind, final byte depth,
      final ValueDictionaryRadixNode oldNode, final int slot, final long childKey) {
    if (slot < 0 || slot >= ValueDictionaryRadixNode.FANOUT || childKey <= 0) {
      throw new IllegalArgumentException("invalid sparse radix child");
    }
    final byte[] oldSlots = oldNode == null
        ? new byte[0]
        : oldNode.getChildSlots();
    final long[] oldKeys = oldNode == null
        ? new long[0]
        : oldNode.getSparseChildKeys();
    int insertion = 0;
    while (insertion < oldSlots.length && Byte.toUnsignedInt(oldSlots[insertion]) < slot) {
      insertion++;
    }
    if (insertion < oldSlots.length && Byte.toUnsignedInt(oldSlots[insertion]) == slot) {
      final long[] keys = oldKeys.clone();
      keys[insertion] = childKey;
      return new ValueDictionaryRadixNode(nodeKey, indexKind, depth, oldSlots, keys);
    }
    final byte[] slots = new byte[oldSlots.length + 1];
    final long[] keys = new long[oldKeys.length + 1];
    System.arraycopy(oldSlots, 0, slots, 0, insertion);
    System.arraycopy(oldKeys, 0, keys, 0, insertion);
    slots[insertion] = (byte) slot;
    keys[insertion] = childKey;
    System.arraycopy(oldSlots, insertion, slots, insertion + 1, oldSlots.length - insertion);
    System.arraycopy(oldKeys, insertion, keys, insertion + 1, oldKeys.length - insertion);
    return new ValueDictionaryRadixNode(nodeKey, indexKind, depth, slots, keys);
  }

  private static ValueDictionaryRadixNode copyRadixNode(final long nodeKey, final ValueDictionaryRadixNode source) {
    if (source == null) {
      throw new IllegalStateException("sparse radix update produced no node");
    }
    return new ValueDictionaryRadixNode(nodeKey, source.getIndexKind(), source.getDepth(), source.getChildSlots(),
        source.getSparseChildKeys());
  }

  private static final class Candidates {
    private long[] hashes;
    private int[] ids;
    private int size;

    private Candidates() {
      hashes = new long[4];
      ids = new int[4];
    }

    private Candidates(final long[] hashes, final int[] ids) {
      this.hashes = hashes;
      this.ids = ids;
      size = ids.length;
    }

    private static Candidates fromAdditions(final IntList localIds, final int oldEntryCount,
        final GlobalValueDictionaryWriter additions) {
      final Candidates candidates = new Candidates();
      for (int i = 0; i < localIds.size; i++) {
        final int localId = localIds.values[i];
        candidates.add(additions.hashAt(localId), Math.addExact(oldEntryCount, localId));
      }
      return candidates;
    }

    private static Candidates concat(final Candidates left, final Candidates right) {
      final Candidates combined = new Candidates(Arrays.copyOf(left.hashes, left.size + right.size),
          Arrays.copyOf(left.ids, left.size + right.size));
      System.arraycopy(right.hashes, 0, combined.hashes, left.size, right.size);
      System.arraycopy(right.ids, 0, combined.ids, left.size, right.size);
      combined.size = left.size + right.size;
      return combined;
    }

    private void add(final long hash, final int id) {
      if (size == ids.length) {
        hashes = Arrays.copyOf(hashes, size << 1);
        ids = Arrays.copyOf(ids, size << 1);
      }
      hashes[size] = hash;
      ids[size] = id;
      size++;
    }

    private int size() {
      return size;
    }

    private long hashAt(final int index) {
      return hashes[index];
    }

    private int idAt(final int index) {
      return ids[index];
    }

    private long[] hashes() {
      return hashes.length == size
          ? hashes.clone()
          : Arrays.copyOf(hashes, size);
    }

    private int[] ids() {
      return ids.length == size
          ? ids.clone()
          : Arrays.copyOf(ids, size);
    }
  }

  private static final class IntList {
    private int[] values = new int[4];
    private int size;
    private ForwardPlan forwardPlan;
    private long writtenLeafKey;

    private void add(final int value) {
      if (size == values.length)
        values = Arrays.copyOf(values, size << 1);
      values[size++] = value;
    }

    private int[] toArray() {
      return Arrays.copyOf(values, size);
    }
  }

  private static final class LongList {
    private long[] values = new long[4];
    private int size;

    private void add(final long value) {
      if (size == values.length)
        values = Arrays.copyOf(values, size << 1);
      values[size++] = value;
    }

    private long[] toArray() {
      return Arrays.copyOf(values, size);
    }
  }

  private static final class KeyCursor {
    private long next;
    private long remainingRecords;

    private KeyCursor(final long first, final long recordCount) {
      if (first <= 0L || recordCount <= 0L) {
        throw new IllegalArgumentException("invalid value dictionary key reservation");
      }
      next = first;
      remainingRecords = recordCount;
    }

    private long next() {
      if (remainingRecords == 0L) {
        throw new IllegalStateException("value dictionary append exceeded its key reservation");
      }
      final long key = next;
      next = Math.addExact(next, RECORD_STRIDE);
      remainingRecords--;
      return key;
    }

    private long peek() {
      if (remainingRecords == 0L) {
        throw new IllegalStateException("value dictionary append exhausted its key reservation");
      }
      return next;
    }

    private void assertExhausted() {
      if (remainingRecords != 0L) {
        throw new IllegalStateException(
            "value dictionary append under-consumed its key reservation by " + remainingRecords + " records");
      }
    }
  }
}
