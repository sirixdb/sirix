/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

/**
 * The parallel importer's fused Stage-A: ONE pass over the raw UTF-8 stream that simultaneously
 * SLICES member-aligned chunks (bulk segment copies, never per-byte appends) and produces every
 * piece of coordinator metadata — exact node count, member count, last-member node count, name
 * occurrences and path resolution — without decoding a single value byte and, in steady state,
 * without allocating per name occurrence:
 *
 * <ul>
 * <li>Names resolve through an open-addressed BYTE-hash table (FNV-1a over the raw name bytes,
 * verified by byte comparison): decode-to-String happens only on a table miss, i.e. once per
 * distinct name for the whole load.</li>
 * <li>Each name entry carries its own tiny {@code parentPCR → fieldPCR} map, so the per-occurrence
 * path cost is one long-hash probe instead of a String-keyed memo lookup; misses resolve through
 * the REAL summary writer on the coordinator (global first-occurrence order preserved:
 * path-resolve before name-intern, the sequential path's exact dictionary call order).</li>
 * <li>Reference-count deltas accumulate per path node and flush per chunk (before any rotation
 * can flush summary pages cold).</li>
 * </ul>
 *
 * <p>
 * The build that follows verifies the count exactly (final minted key == range end AND populated
 * slots == n), so a divergence between this pass and the full tokenizer refuses loudly per chunk.
 */
final class FusedSliceAndScan {

  /**
   * One node of the feeder-owned PATH TRIE: scan context is tracked by trie-node IDENTITY, never
   * by PCR values, so the feeder needs no access to the path summary at all. The coordinator
   * resolves {@link #pcr} for NEW nodes at chunk handoff, in first-occurrence order — the exact
   * order a sequential load inserts paths. Field ownership is disjoint across the two threads:
   * the feeder mutates the children maps and tallies, the coordinator writes {@code pcr}; the
   * handoff queue's put/take pair is the fence that publishes each chunk's new nodes.
   */
  static final class PathStep {
    final PathStep parent;
    /** The STABLE dense name id of this step, or {@code -1} for an {@code __array__} step. */
    final int nameSlot;
    /** The decoded field name (null for {@code __array__} steps); immutable pre-publication. */
    final String name;
    long pcr = -1;
    private Int2ObjectOpenHashMap<PathStep> childrenByNameSlot;
    private PathStep arrayChild;
    /** Occurrences THIS chunk (drained at handoff into reference-count deltas). */
    int chunkOccurrences;
    boolean touchedThisChunk;

    PathStep(final PathStep parent, final int nameSlot, final String name) {
      this.parent = parent;
      this.nameSlot = nameSlot;
      this.name = name;
    }

    Int2ObjectOpenHashMap<PathStep> childrenByNameSlot() {
      if (childrenByNameSlot == null) {
        childrenByNameSlot = new Int2ObjectOpenHashMap<>(4);
      }
      return childrenByNameSlot;
    }

    Int2ObjectOpenHashMap<PathStep> childrenRaw() {
      return childrenByNameSlot;
    }

    PathStep arrayChild() {
      return arrayChild;
    }

    void setArrayChild(final PathStep child) {
      this.arrayChild = child;
    }

  }

  /**
   * One member-aligned chunk plus everything the coordinator must resolve before its build.
   *
   * @param memberNodes the node count of each member in document order, or {@code null} when the
   *        coordinator did not ask for them. Only a load that must name every record — a one-pass
   *        projection build — needs them; an ordinary import pays neither the array nor the stores.
   */
  record Chunk(byte[] bytes, int length, boolean isFinal, long nodes, long members, long lastMemberNodes,
      List<PathStep> newSteps, List<PathStep> touchedSteps, int[] stepOccurrences, List<String> newNames,
      List<int[]> nameTallies, long @Nullable [] memberNodes) {
  }

  private static final int READ_BUFFER_BYTES = 1 << 20;
  private static final int INITIAL_DEPTH_CAPACITY = 64;
  private static final int NAME_TABLE_CAPACITY = 1 << 10;

  private final InputStream in;
  private final int chunkByteBudget;

  private final byte[] readBuffer = new byte[READ_BUFFER_BYTES];
  private int position;
  private int limit;
  /** Start of the not-yet-copied segment of the read buffer (bulk chunk copies). */
  private int segmentStart;

  private byte[] chunkBuffer;
  private int chunkLength;

  private boolean inString;
  private boolean escaped;
  private boolean stringIsName;
  private int nameStartInChunk = -1;
  private boolean arrayClosed;

  // Container context: the current PATH STEP + object flag, mirroring the assembler's rules
  // (plain OBJECT is transparent; arrays add one __array__ step; named containers anchor theirs).
  private PathStep[] contextStep = new PathStep[INITIAL_DEPTH_CAPACITY];
  private boolean[] contextIsObject = new boolean[INITIAL_DEPTH_CAPACITY];
  private int depth;

  /** A name string just closed; the NEXT value token binds to it (its path step precomputed). */
  private PathStep pendingFieldStep;
  private boolean namePending;

  /** The document-ordered handoff lists for the CURRENT chunk. */
  private final ArrayList<PathStep> newSteps = new ArrayList<>();
  private final ArrayList<PathStep> touchedSteps = new ArrayList<>();
  private final ArrayList<String> newNames = new ArrayList<>();

  private long nodes;
  private long members;
  private long lastMemberNodes;
  private long memberStartNodes;

  /**
   * Per-member node counts for the CURRENT chunk, or {@code null} when the coordinator does not
   * need them. Reserved once at the requested capacity and reused for every chunk, so a load that
   * does need them still allocates no per-chunk array beyond the handed-off copy.
   */
  private final @Nullable LongArrayList memberNodes;

  // ==== open-addressed byte-hash name table (slots map to STABLE dense ids) ===================
  private byte[][] nameBytes = new byte[NAME_TABLE_CAPACITY][];
  private int[] nameIdBySlot = new int[NAME_TABLE_CAPACITY];
  private String[] nameStringById = new String[64];
  private int[] nameChunkOccurrencesById = new int[64];
  private int nameCount;
  private int nameMask = NAME_TABLE_CAPACITY - 1;

  /** Distinct-name slots touched THIS chunk, for occurrence-delta emission + reset. */
  private int[] touchedNameSlots = new int[256];
  private int touchedNameCount;

  /** The trie root = the top-level array's own step; the coordinator sets its PCR up front. */
  private final PathStep rootStep = new PathStep(null, -1, null);

  /**
   * Recycled chunk buffers: a fresh multi-MB {@code byte[]} per chunk is a G1 humongous
   * allocation — measured at ~2.8 GB of churn every 1.7 s on a 1M import, driving 10 of 12
   * collections. Receivers return buffers via {@link #releaseChunkBuffer} once the build is done.
   */
  private final ArrayBlockingQueue<byte[]> chunkBufferPool = new ArrayBlockingQueue<>(16);

  private byte[] acquireChunkBuffer(final int needed) {
    final byte[] pooled = chunkBufferPool.poll();
    if (pooled != null && pooled.length >= needed) {
      return pooled;
    }
    return new byte[Math.max(needed, chunkByteBudget + (chunkByteBudget >> 2))];
  }

  /** Thread-safe; called by the importer (any thread) when a chunk's bytes are no longer read. */
  void releaseChunkBuffer(final byte[] buffer) {
    chunkBufferPool.offer(buffer);
  }

  FusedSliceAndScan(final InputStream in, final int chunkByteBudget) {
    this(in, chunkByteBudget, false);
  }

  /**
   * @param trackMemberNodes whether each chunk should carry its members' node counts — the only way
   *        a coordinator can name every record's root key by range arithmetic
   */
  FusedSliceAndScan(final InputStream in, final int chunkByteBudget, final boolean trackMemberNodes) {
    this.in = in;
    this.chunkByteBudget = Math.max(1024, chunkByteBudget);
    this.chunkBuffer = new byte[this.chunkByteBudget + (this.chunkByteBudget >> 2)];
    this.memberNodes = trackMemberNodes
        ? new LongArrayList(1024)
        : null;
  }

  PathStep rootStep() {
    return rootStep;
  }

  /** Consumes leading whitespace and the top-level {@code '['}. */
  void consumeArrayOpen() throws IOException {
    while (true) {
      if (position == limit && !fill()) {
        throw new IllegalStateException("input ended before the top-level array opened");
      }
      final byte c = readBuffer[position];
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
        position++;
        segmentStart = position;
        continue;
      }
      if (c != '[') {
        throw new IllegalStateException("parallel import requires a top-level array, got byte " + c);
      }
      position++;
      segmentStart = position;
      return;
    }
  }

  /** The next member-aligned chunk with its metadata, or {@code null} at the array's end. */
  Chunk nextChunk() throws IOException {
    if (arrayClosed) {
      return null;
    }
    chunkLength = 0;
    nodes = 0;
    members = 0;
    lastMemberNodes = 0;
    memberStartNodes = 0;
    if (memberNodes != null) {
      memberNodes.clear();
    }
    depth = 0;
    contextStep[0] = rootStep;
    contextIsObject[0] = false;
    namePending = false;
    newSteps.clear();
    touchedSteps.clear();
    newNames.clear();

    while (true) {
      if (position == limit && !refillPreservingChunk()) {
        throw new IllegalStateException("input ended inside the top-level array (depth " + depth + ")");
      }
      final byte c = readBuffer[position];

      if (inString) {
        if (escaped) {
          escaped = false;
          position++;
          continue;
        }
        // Tight skip: race through the string body to the next quote or backslash — the bulk of
        // real-world bytes live inside string values, and this loop is the whole cost of passing
        // them (no decode, no copy beyond the segment flush).
        final byte[] buffer = readBuffer;
        int scanPosition = position;
        final int scanLimit = limit;
        while (scanPosition < scanLimit) {
          final byte b = buffer[scanPosition];
          if (b == '"' || b == '\\') {
            break;
          }
          scanPosition++;
        }
        position = scanPosition;
        if (position == limit) {
          continue; // refill via the loop head
        }
        if (buffer[position] == '\\') {
          escaped = true;
          position++;
          continue;
        }
        // Closing quote.
        inString = false;
        position++;
        if (stringIsName) {
          stringIsName = false;
          // The name's bytes sit in the CHUNK buffer (flushed below) or the current segment;
          // close the segment so the name is contiguous in chunkBuffer, then resolve it.
          flushSegment();
          resolveName(nameStartInChunk, chunkLength - 1);
          nameStartInChunk = -1;
        } else {
          nodes++;
          valueCompleted();
        }
        continue;
      }

      switch (c) {
        case ' ', '\t', '\n', '\r', ',', ':' -> position++;
        case '"' -> {
          inString = true;
          // Name iff directly inside an object and no name is pending yet.
          stringIsName = contextIsObject[depth] && !namePending;
          if (stringIsName) {
            flushSegmentUpTo(position);
            nameStartInChunk = chunkLength + 1;
          }
          position++;
        }
        case '{' -> {
          nodes++;
          if (namePending) {
            namePending = false;
            push(pendingFieldStep, true);
          } else {
            // Plain OBJECT is transparent for path anchoring.
            push(contextStep[depth], true);
          }
          position++;
        }
        case '[' -> {
          nodes++;
          if (namePending) {
            namePending = false;
            push(arrayStepUnder(pendingFieldStep), false);
          } else {
            push(arrayStepUnder(contextStep[depth]), false);
          }
          position++;
        }
        case '}' -> {
          if (depth == 0) {
            throw new IllegalStateException("unbalanced '}' at the top level");
          }
          depth--;
          position++;
          if (depth == 0) {
            memberCompleted();
            final Chunk chunk = maybeCloseChunk();
            if (chunk != null) {
              return chunk;
            }
          }
        }
        case ']' -> {
          if (depth == 0) {
            arrayClosed = true;
            flushSegmentUpTo(position);
            position++;
            segmentStart = position;
            return nodes > 0
                ? finishChunk(true)
                : null;
          }
          depth--;
          position++;
          if (depth == 0) {
            memberCompleted();
            final Chunk chunk = maybeCloseChunk();
            if (chunk != null) {
              return chunk;
            }
          }
        }
        default -> {
          // Number or literal: skip to its structural end.
          nodes++;
          if (namePending) {
            namePending = false;
          }
          position++;
          while (true) {
            if (position == limit && !refillPreservingChunk()) {
              throw new IllegalStateException("input ended inside a literal");
            }
            final byte v = readBuffer[position];
            if (v == ',' || v == '}' || v == ']' || v == ' ' || v == '\t' || v == '\n' || v == '\r') {
              break;
            }
            position++;
          }
          if (depth == 0) {
            memberCompleted();
            final Chunk chunk = maybeCloseChunk();
            if (chunk != null) {
              return chunk;
            }
          }
        }
      }
    }
  }

  private void valueCompleted() {
    if (namePending) {
      namePending = false;
    }
    if (depth == 0) {
      memberCompleted();
    }
  }

  private void memberCompleted() {
    members++;
    lastMemberNodes = nodes - memberStartNodes;
    memberStartNodes = nodes;
    if (memberNodes != null) {
      memberNodes.add(lastMemberNodes);
    }
  }

  /** Closes the chunk at this member boundary when the budget is met. */
  private Chunk maybeCloseChunk() throws IOException {
    flushSegmentUpTo(position);
    if (chunkLength < chunkByteBudget) {
      return null;
    }
    final boolean isFinal = peekArrayCloses();
    return finishChunk(isFinal);
  }

  private boolean peekArrayCloses() throws IOException {
    while (true) {
      if (position == limit && !fill()) {
        throw new IllegalStateException("input ended inside the top-level array");
      }
      final byte c = readBuffer[position];
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',') {
        position++;
        segmentStart = position;
        continue;
      }
      if (c == ']') {
        arrayClosed = true;
        position++;
        segmentStart = position;
        return true;
      }
      return false;
    }
  }

  // ==== chunk buffer (bulk segment copies) =====================================================

  private void flushSegment() {
    flushSegmentUpTo(position);
  }

  private void flushSegmentUpTo(final int end) {
    final int len = end - segmentStart;
    if (len > 0) {
      ensureChunkCapacity(len);
      System.arraycopy(readBuffer, segmentStart, chunkBuffer, chunkLength, len);
      chunkLength += len;
    }
    segmentStart = end;
  }

  private void ensureChunkCapacity(final int extra) {
    if (chunkLength + extra > chunkBuffer.length) {
      chunkBuffer = Arrays.copyOf(chunkBuffer, Math.max(chunkBuffer.length << 1, chunkLength + extra));
    }
  }

  private boolean fill() throws IOException {
    flushSegmentUpTo(limit);
    limit = in.read(readBuffer, 0, readBuffer.length);
    position = 0;
    segmentStart = 0;
    return limit > 0;
  }

  private boolean refillPreservingChunk() throws IOException {
    return fill();
  }

  // ==== name resolution ========================================================================

  /** Name bytes live at {@code chunkBuffer[start .. endExclusive)} (closing quote excluded). */
  private void resolveName(final int start, final int endExclusive) {
    final int len = endExclusive - start;
    long hash = 0xcbf29ce484222325L;
    for (int i = start; i < endExclusive; i++) {
      hash = (hash ^ chunkBuffer[i]) * 0x100000001b3L;
    }
    int slot = (int) hash & nameMask;
    while (true) {
      final byte[] existing = nameBytes[slot];
      if (existing == null) {
        insertName(slot, start, len);
        break;
      }
      if (matches(existing, start, len)) {
        break;
      }
      slot = (slot + 1) & nameMask;
    }
    final int nameId = nameIdBySlot[slot];
    if (nameChunkOccurrencesById[nameId]++ == 0) {
      rememberTouched(nameId);
    }
    pendingFieldStep = fieldStepUnder(contextStep[depth], nameId);
    namePending = true;
  }

  /** Get-or-create the named child step; occurrences tally per step for reference deltas. */
  private PathStep fieldStepUnder(final PathStep parent, final int nameId) {
    final Int2ObjectOpenHashMap<PathStep> children = parent.childrenByNameSlot();
    PathStep step = children.get(nameId);
    if (step == null) {
      step = new PathStep(parent, nameId, nameStringById[nameId]);
      children.put(nameId, step);
      newSteps.add(step);
    }
    tallyStep(step);
    return step;
  }

  private PathStep arrayStepUnder(final PathStep parent) {
    PathStep step = parent.arrayChild();
    if (step == null) {
      step = new PathStep(parent, -1, null);
      parent.setArrayChild(step);
      newSteps.add(step);
    }
    tallyStep(step);
    return step;
  }

  private void tallyStep(final PathStep step) {
    if (!step.touchedThisChunk) {
      step.touchedThisChunk = true;
      touchedSteps.add(step);
    }
    step.chunkOccurrences++;
  }

  private Chunk finishChunk(final boolean isFinal) {
    final List<int[]> tallies = new ArrayList<>(touchedNameCount);
    for (int i = 0; i < touchedNameCount; i++) {
      final int nameId = touchedNameSlots[i];
      tallies.add(new int[] { nameId, nameChunkOccurrencesById[nameId] });
      nameChunkOccurrencesById[nameId] = 0;
    }
    touchedNameCount = 0;
    final int[] stepOccurrences = new int[touchedSteps.size()];
    for (int i = 0; i < touchedSteps.size(); i++) {
      final PathStep step = touchedSteps.get(i);
      stepOccurrences[i] = step.chunkOccurrences;
      step.chunkOccurrences = 0;
      step.touchedThisChunk = false;
    }
    final byte[] out = acquireChunkBuffer(chunkLength);
    System.arraycopy(chunkBuffer, 0, out, 0, chunkLength);
    final long[] memberNodeCounts = memberNodes == null
        ? null
        : memberNodes.toLongArray();
    return new Chunk(out, chunkLength, isFinal, nodes, members, lastMemberNodes, List.copyOf(newSteps),
        List.copyOf(touchedSteps), stepOccurrences, List.copyOf(newNames), tallies, memberNodeCounts);
  }

  private boolean matches(final byte[] existing, final int start, final int len) {
    if (existing.length != len) {
      return false;
    }
    return Arrays.mismatch(existing, 0, len, chunkBuffer, start, start + len) < 0;
  }

  private void insertName(final int slot, final int start, final int len) {
    final byte[] copy = new byte[len];
    System.arraycopy(chunkBuffer, start, copy, 0, len);
    final String decoded = decodeName(copy);
    final int nameId = nameCount++;
    nameBytes[slot] = copy;
    nameIdBySlot[slot] = nameId;
    if (nameId == nameStringById.length) {
      nameStringById = Arrays.copyOf(nameStringById, nameStringById.length << 1);
      nameChunkOccurrencesById = Arrays.copyOf(nameChunkOccurrencesById, nameChunkOccurrencesById.length << 1);
    }
    nameStringById[nameId] = decoded;
    newNames.add(decoded);
    if (nameCount * 2 > nameMask) {
      growNameTable();
    }
  }

  private void rememberTouched(final int slot) {
    if (touchedNameCount == touchedNameSlots.length) {
      touchedNameSlots = Arrays.copyOf(touchedNameSlots, touchedNameSlots.length << 1);
    }
    touchedNameSlots[touchedNameCount++] = slot;
  }

  private void growNameTable() {
    final int newCapacity = (nameMask + 1) << 1;
    final byte[][] oldBytes = nameBytes;
    final int[] oldIds = nameIdBySlot;
    nameBytes = new byte[newCapacity][];
    nameIdBySlot = new int[newCapacity];
    nameMask = newCapacity - 1;
    for (int i = 0; i < oldBytes.length; i++) {
      final byte[] name = oldBytes[i];
      if (name == null) {
        continue;
      }
      long hash = 0xcbf29ce484222325L;
      for (final byte b : name) {
        hash = (hash ^ b) * 0x100000001b3L;
      }
      int slot = (int) hash & nameMask;
      while (nameBytes[slot] != null) {
        slot = (slot + 1) & nameMask;
      }
      nameBytes[slot] = name;
      nameIdBySlot[slot] = oldIds[i];
    }
  }

  private void push(final PathStep step, final boolean isObject) {
    depth++;
    if (depth == contextStep.length) {
      contextStep = Arrays.copyOf(contextStep, contextStep.length << 1);
      contextIsObject = Arrays.copyOf(contextIsObject, contextIsObject.length << 1);
    }
    contextStep[depth] = step;
    contextIsObject[depth] = isObject;
  }

  private static String decodeName(final byte[] raw) {
    final String utf8 = new String(raw, java.nio.charset.StandardCharsets.UTF_8);
    if (utf8.indexOf('\\') < 0) {
      return utf8;
    }
    final StringBuilder out = new StringBuilder(utf8.length());
    for (int i = 0; i < utf8.length(); i++) {
      char c = utf8.charAt(i);
      if (c == '\\') {
        i++;
        c = utf8.charAt(i);
        switch (c) {
          case '"', '\\', '/' -> out.append(c);
          case 'b' -> out.append('\b');
          case 'f' -> out.append('\f');
          case 'n' -> out.append('\n');
          case 'r' -> out.append('\r');
          case 't' -> out.append('\t');
          case 'u' -> {
            out.append((char) Integer.parseInt(utf8.substring(i + 1, i + 5), 16));
            i += 4;
          }
          default -> throw new IllegalStateException("invalid escape \\" + c + " in field name");
        }
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }
}
