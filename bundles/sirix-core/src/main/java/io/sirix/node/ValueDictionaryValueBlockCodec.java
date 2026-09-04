package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.SirixLZ77Codec;
import io.sirix.page.SirixLZ77NativeDecoder;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * The compact storage forms of {@link ValueDictionaryValueBlockNode}, and the only place that knows
 * a block is stored as anything other than a plain run of bytes.
 *
 * <p>
 * A block holds up to 256 values packed ascending by id. Once ids are assigned in COLLATION order —
 * which is what the rank pass exists to establish — those 256 neighbours share long prefixes, and
 * two further forms become worth their framing:
 * </p>
 *
 * <ul>
 * <li><b>FRONT-CODED</b> — each entry becomes {@code (uvarint sharedPrefixLen, uvarint suffixLen)}
 * plus its suffix, measured against the PREVIOUS entry of the same block only, so a block stays
 * independently decodable. Discriminated by a NEGATIVE {@code byteLength}.</li>
 * <li><b>FRONT-CODED + LZ77</b> — the whole post-header payload as one {@link SirixLZ77Codec}
 * frame. Discriminated by a NEGATIVE {@code count}. This form exists because a 256-value block is
 * ~33 KB, far above {@code Constants.MAX_RECORD_SIZE}, so it is diverted to an {@code OverflowPage}
 * and the page body codec never sees it: measured {@code written/raw = 1.001}. Compressing inside
 * the record is the only place the dictionary can reach, and it was worth 0.52 to 0.33 of raw value
 * bytes at D = 2.62M.</li>
 * </ul>
 *
 * <p>
 * <b>The decoded shape and the whole read API are unchanged.</b> Every form reconstructs
 * {@code (offsets, bytes)} and hands them to {@code takeOwnership}, so {@code rawBytes()},
 * {@code valueOffset(id)} and {@code valueLength(id)} — and therefore every kernel above them — do
 * not learn that the stored form moved.
 * </p>
 *
 * <p>
 * Election is per block, by MEASURED size, and a tie writes the simpler form. With the kill switch
 * off no candidate is even built, so "off" is byte-identical to the encoder that predates this.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class ValueDictionaryValueBlockCodec {

  /**
   * Segment 1's master switch, and it is OFF by default.
   *
   * <p>
   * The compact forms only pay in rank order, and rank order only exists once the pass has run — so
   * gating the encoder on the same property that gates the pass makes the whole of segment 1 inert
   * unless it is asked for. That is what lets "off" be proven byte-identical to a build that predates
   * this work: no candidate is built, so the encoder emits exactly the plain form it always did.
   * </p>
   *
   * <p>
   * The DECODERS are never gated. A resource written with the switch on must stay readable with it
   * off, or the switch would be a data-loss lever rather than a kill switch.
   * </p>
   */
  static final boolean ENABLED = Boolean.getBoolean("sirix.projection.globalDict.rank");

  /**
   * Whether the front-coded payload is additionally offered as one LZ77 frame. Separable from
   * {@link #ENABLED} only so the two forms can be measured against each other; both default on.
   */
  static final boolean COMPRESSED =
      ENABLED && !"false".equalsIgnoreCase(System.getProperty("sirix.projection.globalDict.blockCompression", "true"));

  private ValueDictionaryValueBlockCodec() {
    throw new AssertionError("no instances");
  }

  /**
   * Election is per block, by measured size: build the front-coded candidate, and emit it only when
   * it is strictly smaller than the plain form. A tie writes plain.
   *
   * @return {@code true} when the front-coded form was written and the caller must not write the
   *         plain one
   */
  static boolean serializeIfSmaller(final BytesOut<?> sink, final ValueDictionaryValueBlockNode node) {
    final byte[] bytes = node.rawBytes();
    final int count = node.size();
    // Header (firstId, count, byteLength) is common to both forms and cancels out of the election.
    final long plainPayload = (long) count * Integer.BYTES + bytes.length;

    final int[] shared = new int[count];
    final int[] suffix = new int[count];
    long metaBytes = 0L;
    long suffixTotal = 0L;
    int previousStart = 0;
    int previousLength = 0;
    for (int i = 0; i < count; i++) {
      final int start = node.offsetAt(i);
      final int length = node.offsetAt(i + 1) - start;
      int common = 0;
      if (i > 0) {
        final int limit = Math.min(previousLength, length);
        while (common < limit && bytes[previousStart + common] == bytes[start + common]) {
          common++;
        }
      }
      shared[i] = common;
      suffix[i] = length - common;
      metaBytes += unsignedVarIntLength(common) + unsignedVarIntLength(length - common);
      suffixTotal += length - common;
      previousStart = start;
      previousLength = length;
    }

    // A zero-length suffix region would serialize as byteLength 0, which reads back as the PLAIN
    // form. The two forms must stay distinguishable, so such a block is written plain.
    if (suffixTotal == 0L) {
      return false;
    }
    if (COMPRESSED) {
      return serializeCompressed(sink, node, shared, suffix, count, Math.toIntExact(metaBytes + suffixTotal),
          Math.toIntExact(plainPayload));
    }
    if (metaBytes + suffixTotal >= plainPayload) {
      return false;
    }

    sink.writeInt(node.getFirstId());
    sink.writeInt(count);
    sink.writeInt(Math.negateExact(Math.toIntExact(suffixTotal)));
    for (int i = 0; i < count; i++) {
      writeUnsignedVarInt(sink, shared[i]);
    }
    for (int i = 0; i < count; i++) {
      writeUnsignedVarInt(sink, suffix[i]);
    }
    for (int i = 0; i < count; i++) {
      final int start = node.offsetAt(i);
      sink.write(bytes, start + shared[i], suffix[i]);
    }
    return true;
  }

  /**
   * Builds the front-coded payload, compresses it, and emits it only when it beats both other forms.
   */
  private static boolean serializeCompressed(final BytesOut<?> sink, final ValueDictionaryValueBlockNode node,
      final int[] shared, final int[] suffix, final int count, final int payloadLength, final int plainPayload) {
    final byte[] payload = new byte[payloadLength];
    int at = 0;
    for (int i = 0; i < count; i++) {
      at = writeVarIntTo(payload, at, shared[i]);
    }
    for (int i = 0; i < count; i++) {
      at = writeVarIntTo(payload, at, suffix[i]);
    }
    final byte[] bytes = node.rawBytes();
    for (int i = 0; i < count; i++) {
      final int start = node.offsetAt(i) + shared[i];
      System.arraycopy(bytes, start, payload, at, suffix[i]);
      at += suffix[i];
    }
    if (at != payloadLength) {
      throw new IllegalStateException("front-coded payload length disagrees with its plan");
    }
    final byte[] compressed = new byte[SirixLZ77Codec.maxEncodedSize(payloadLength)];
    final int compressedLength =
        SirixLZ77Codec.encode(MemorySegment.ofArray(payload), 0L, payloadLength, compressed, 0);
    // Election across all three forms, by measured size; ties fall back to the simpler form.
    if (compressedLength + Integer.BYTES >= Math.min(payloadLength, plainPayload)) {
      return false;
    }
    sink.writeInt(node.getFirstId());
    sink.writeInt(-count);
    sink.writeInt(compressedLength);
    sink.writeInt(payloadLength);
    sink.write(compressed, 0, compressedLength);
    return true;
  }

  /** Reads back the compressed front-coded form. Self-describing: the caller saw a negative count. */
  static DataRecord deserializeCompressed(final BytesIn<?> source, final long recordID, final int firstId,
      final int count) {
    final int compressedLength = source.readInt();
    final int payloadLength = source.readInt();
    if (compressedLength < 0 || payloadLength < 0 || compressedLength > source.remaining()
        || payloadLength > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES + (count * 8)) {
      throw new IllegalStateException("invalid compressed value dictionary value block header");
    }
    final byte[] compressed = new byte[compressedLength + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK];
    source.read(compressed, 0, compressedLength);
    final byte[] payload = new byte[payloadLength];
    final int decoded = decodeInto(compressed, compressedLength, payload, payloadLength);
    if (decoded != payloadLength) {
      throw new IllegalStateException("compressed value dictionary value block decoded to the wrong length");
    }
    return decodeFrontCoded(new ArrayCursor(payload), recordID, firstId, count, -1);
  }

  /**
   * Per-thread NATIVE landing area for a frame being decoded.
   *
   * <p>
   * {@link SirixLZ77Codec#decode} dispatches to the C decoder only when its output is native-backed
   * with tail slack; a heap output silently takes the Java decoder, measured at <b>3.0 GB/s against
   * 16.9</b> on a 32 KB frame — and a 256-value block is ~33 KB, squarely in that regime. This is not
   * only the probe path: it is reached by EVERY read of a compressed block, so it is paid by any
   * query that materialises strings from a global-dictionary column.
   * </p>
   *
   * <p>
   * {@link Arena#ofAuto()} rather than a confined arena: the segment is reachable only from this
   * thread-local, so it is freed with the thread and there is no {@code close()} to get wrong on a
   * pool thread that outlives any one record.
   * </p>
   */
  private static final ThreadLocal<MemorySegment> NATIVE_LANDING =
      ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(1 << 16));

  /** Below this the detour's extra copy costs more than the faster decoder saves. */
  private static final int NATIVE_DECODE_MIN_BYTES = 1 << 10;

  /**
   * Decodes one frame into {@code target}, through a native landing area when that is faster.
   *
   * <p>
   * <b>The landing area never escapes this method.</b> It is a reused per-thread buffer, so handing a
   * slice of it to a cached page or a retained block view would be a use-after-free the moment the
   * next record decodes on the same thread. The bytes are copied out and the view is dropped; the
   * copy runs at memcpy speed and is bought back several times over (0.16 ns/B for native decode plus
   * the copy, against 0.33 ns/B for a heap decode).
   * </p>
   *
   * @return the number of bytes produced, which the caller checks against what the writer recorded
   */
  private static int decodeInto(final byte[] compressed, final int compressedLength, final byte[] target,
      final int targetLength) {
    if (targetLength < NATIVE_DECODE_MIN_BYTES || !SirixLZ77NativeDecoder.isAvailable()) {
      return SirixLZ77Codec.decode(compressed, 0, compressedLength, MemorySegment.ofArray(target), 0L);
    }
    MemorySegment landing = NATIVE_LANDING.get();
    // NATIVE_OUTPUT_TAIL_SLACK, not the INPUT constant — the dispatch tests
    // `outputOff + uncompressed + NATIVE_OUTPUT_TAIL_SLACK <= output.byteSize()`, and the two
    // constants differ (64 against 16). Sizing the landing with the input slack leaves it 48 bytes
    // short, the dispatch silently declines, and the whole detour buys nothing while costing a copy.
    final long needed = (long) targetLength + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK;
    if (landing.byteSize() < needed) {
      landing = Arena.ofAuto().allocate(Math.max(needed, landing.byteSize() * 2));
      NATIVE_LANDING.set(landing);
    }
    final int produced = SirixLZ77Codec.decode(compressed, 0, compressedLength, landing, 0L);
    if (produced == targetLength) {
      MemorySegment.copy(landing, ValueLayout.JAVA_BYTE, 0L, target, 0, targetLength);
    }
    return produced;
  }

  private static int writeVarIntTo(final byte[] sink, final int at, final int value) {
    int written = at;
    int remaining = value;
    while ((remaining & ~0x7F) != 0) {
      sink[written++] = (byte) ((remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }
    sink[written++] = (byte) remaining;
    return written;
  }

  /** A minimal byte cursor so the front-coded decoder serves both the framed and compressed forms. */
  private interface ByteCursor {
    int readByte();

    long remaining();

    void read(byte[] target, int offset, int length);
  }

  private static final class ArrayCursor implements ByteCursor {
    private final byte[] bytes;
    private int at;

    private ArrayCursor(final byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int readByte() {
      return bytes[at++];
    }

    @Override
    public long remaining() {
      return bytes.length - at;
    }

    @Override
    public void read(final byte[] target, final int offset, final int length) {
      System.arraycopy(bytes, at, target, offset, length);
      at += length;
    }
  }

  private record SourceCursor(BytesIn<?> source) implements ByteCursor {
    @Override
    public int readByte() {
      return source.readByte();
    }

    @Override
    public long remaining() {
      return source.remaining();
    }

    @Override
    public void read(final byte[] target, final int offset, final int length) {
      source.read(target, offset, length);
    }
  }

  /**
   * Reconstructs {@code (offsets, bytes)} from the front-coded form in one forward pass, so the
   * decoded shape and the whole read API are unchanged.
   *
   * <p>
   * Every claimed size is bounded BEFORE anything is allocated, and a shared prefix longer than the
   * previous entry is refused: a corrupt record must be able neither to reserve arbitrary memory nor
   * to read past the value it front-codes against.
   */
  static DataRecord deserializeFrontCoded(final BytesIn<?> source, final long recordID, final int firstId,
      final int count, final int suffixRegionLength) {
    return decodeFrontCoded(new SourceCursor(source), recordID, firstId, count, suffixRegionLength);
  }

  private static DataRecord decodeFrontCoded(final ByteCursor source, final long recordID, final int firstId,
      final int count, final int suffixRegionLength) {
    final int[] shared = new int[count];
    final int[] suffix = new int[count];
    for (int i = 0; i < count; i++) {
      shared[i] = readUnsignedVarInt(source);
    }
    for (int i = 0; i < count; i++) {
      suffix[i] = readUnsignedVarInt(source);
    }
    long decoded = 0L;
    long suffixTotal = 0L;
    int previousLength = 0;
    for (int i = 0; i < count; i++) {
      if (shared[i] > previousLength) {
        throw new IllegalStateException("value dictionary front-coded prefix overruns the previous value");
      }
      final long length = (long) shared[i] + suffix[i];
      decoded += length;
      suffixTotal += suffix[i];
      if (decoded > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES) {
        throw new IllegalStateException("value dictionary front-coded block decodes above the block ceiling");
      }
      previousLength = (int) length;
    }
    if (suffixRegionLength >= 0 && suffixTotal != suffixRegionLength) {
      throw new IllegalStateException("value dictionary front-coded suffix region disagrees with its lengths");
    }
    final int suffixBytes = Math.toIntExact(suffixTotal);
    if (suffixBytes > source.remaining()) {
      throw new IllegalStateException("value dictionary front-coded block overruns its record");
    }

    final byte[] suffixes = new byte[suffixBytes];
    if (suffixBytes > 0) {
      source.read(suffixes, 0, suffixBytes);
    }
    final int[] offsets = new int[count + 1];
    final byte[] bytes = new byte[(int) decoded];
    int out = 0;
    int suffixAt = 0;
    for (int i = 0; i < count; i++) {
      if (shared[i] > 0) {
        System.arraycopy(bytes, offsets[i - 1], bytes, out, shared[i]);
        out += shared[i];
      }
      System.arraycopy(suffixes, suffixAt, bytes, out, suffix[i]);
      suffixAt += suffix[i];
      out += suffix[i];
      offsets[i + 1] = out;
    }
    return ValueDictionaryValueBlockNode.takeOwnership(recordID, firstId, offsets, bytes);
  }

  private static int unsignedVarIntLength(final int value) {
    if (value < 0) {
      throw new IllegalArgumentException("negative varint: " + value);
    }
    if (value < 1 << 7) {
      return 1;
    }
    if (value < 1 << 14) {
      return 2;
    }
    if (value < 1 << 21) {
      return 3;
    }
    if (value < 1 << 28) {
      return 4;
    }
    return 5;
  }

  private static void writeUnsignedVarInt(final BytesOut<?> sink, final int value) {
    int remaining = value;
    while ((remaining & ~0x7F) != 0) {
      sink.writeByte((byte) ((remaining & 0x7F) | 0x80));
      remaining >>>= 7;
    }
    sink.writeByte((byte) remaining);
  }

  private static int readUnsignedVarInt(final ByteCursor source) {
    int result = 0;
    for (int shift = 0; shift <= 28; shift += 7) {
      final int read = source.readByte() & 0xFF;
      result |= (read & 0x7F) << shift;
      if ((read & 0x80) == 0) {
        if (result < 0) {
          throw new IllegalStateException("value dictionary front-coded varint overflows an int");
        }
        return result;
      }
    }
    throw new IllegalStateException("value dictionary front-coded varint is not terminated");
  }
}
