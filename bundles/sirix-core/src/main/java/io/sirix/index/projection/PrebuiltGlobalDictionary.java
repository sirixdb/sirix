package io.sirix.index.projection;

import io.sirix.api.StorageEngineWriter;
import io.sirix.node.ValueDictionaryHeaderNode;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Resolves values against a dictionary that is ALREADY COMPLETE — it never mints an id.
 *
 * <p>
 * This is what lets a build write a rank-ordered column without ever constructing a forward hash
 * index. The streaming path has to answer "have I seen this value" while it reads, so it must hold
 * every distinct value plus a probe index, and it persists a forward radix whose measured cost is
 * <b>1,650 B per entry</b> — 0.81 radix nodes per entry, each carrying a 256-slot child array —
 * where the same column written from a finished rank-ordered dictionary costs <b>61 B per
 * entry</b>. A pre-pass that sorts and ranks the whole value set before the build starts makes that
 * index unnecessary rather than merely smaller.
 * </p>
 *
 * <p>
 * <b>An absent value is a build error, not a value to mint.</b> The dictionary was built from the
 * same corpus this build is reading, so a miss means the pre-pass and the build disagree about the
 * value set — a wrong id lane, silently, if it were papered over by appending. It is refused by
 * name, and so is {@code ID_UNKNOWN}, which means "I cannot see the dictionary" and must never be
 * confused with "the value is not there".
 * </p>
 *
 * <p>
 * <b>Cost, and where this shape stops working.</b> A probe is one block decode plus a walk of the
 * separator array. Attached to the per-leaf conversion path it is paid once per per-leaf DICTIONARY
 * ENTRY and memoised across leaves, not once per row: measured at 1M that is ~600k probes for URL
 * and Title together against 2M row-level calls. At 100M the same route would be ~95M probes, which
 * is minutes, and the build should take its ids positionally from the pre-pass instead — the
 * pre-pass already visits the values in row order. This class is the right answer at the scale
 * where probing is cheaper than the coupling, and the wrong one above it.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class PrebuiltGlobalDictionary implements GlobalValueDictionaryEncoder {

  private final int column;

  private final long headerKey;

  private final StorageEngineWriter storageEngineWriter;

  private final int entryCount;

  /** Value memo across leaves; the per-leaf dictionaries overlap heavily on a real corpus. */
  private final GlobalValueDictionaryHotCache hotValues = new GlobalValueDictionaryHotCache();

  private long probes;

  private long hits;

  PrebuiltGlobalDictionary(final int column, final long headerKey, final StorageEngineWriter storageEngineWriter) {
    if (column < 0) {
      throw new IllegalArgumentException("column must not be negative: " + column);
    }
    if (headerKey <= 0) {
      throw new IllegalArgumentException("a prebuilt dictionary needs its header key, not " + headerKey);
    }
    final ValueDictionaryHeaderNode header =
        GlobalValueDictionary.header(headerKey, Objects.requireNonNull(storageEngineWriter, "writer"));
    if (header == null || !header.isDirectoryComplete()) {
      throw new IllegalStateException("global projection column " + column + " has an unreadable value dictionary");
    }
    if (!header.isFullyOrdered()) {
      throw new IllegalStateException("global projection column " + column + " was handed a dictionary whose ids are "
          + "not all in collation order (" + header.getOrderedPrefixCount() + " of " + header.getEntryCount()
          + "); a prebuilt dictionary is only worth injecting when it is rank-ordered");
    }
    this.column = column;
    this.headerKey = headerKey;
    this.storageEngineWriter = storageEngineWriter;
    this.entryCount = header.getEntryCount();
  }

  @Override
  public int intern(final String value) {
    final byte[] utf8 = (value == null
        ? ""
        : value).getBytes(StandardCharsets.UTF_8);
    return intern(utf8, 0, utf8.length);
  }

  @Override
  public int intern(final byte[] source, final int offset, final int length) {
    Objects.checkFromIndexSize(offset, length, source.length);
    final int cached = hotValues.find(source, offset, length);
    if (cached > 0) {
      hits++;
      return cached;
    }
    probes++;
    final int id = GlobalValueDictionary.probe(headerKey, source, offset, length, storageEngineWriter);
    if (id > 0) {
      hotValues.put(source, offset, length, id);
      return id;
    }
    // The VALUE is in the message, not just the fact of its absence. A disagreement between the
    // pre-pass and the build is a difference in how two readers of the same corpus normalise one
    // string, and the only way to see which normalisation differs is to look at the string: without
    // it the next step is guessing, and a guess costs a whole rebuild to test.
    final String shown = new String(source, offset, Math.min(length, 200), StandardCharsets.UTF_8);
    final StringBuilder hex = new StringBuilder(3 * Math.min(length, 64));
    for (int i = 0; i < Math.min(length, 64); i++) {
      hex.append(String.format("%02x ", source[offset + i]));
    }
    throw new IllegalStateException("global projection column " + column + " met a value its prebuilt dictionary of "
        + entryCount + " entries does not hold (" + (id == GlobalValueDictionary.ID_ABSENT
            ? "absent"
            : "unreadable")
        + "). The pre-pass and this build disagree about the value set; appending it here would "
        + "put an id in the lane that no reader can resolve in collation order." + " length=" + length + " value=["
        + shown + "] firstBytes=[" + hex.toString().trim() + ']');
  }

  /** The dictionary this column resolves against, for the metadata anchor. */
  long headerKey() {
    return headerKey;
  }

  int column() {
    return column;
  }

  /** Persistent probes taken and memo hits served — the cost this shape trades against coupling. */
  long probeCount() {
    return probes;
  }

  long hitCount() {
    return hits;
  }
}
