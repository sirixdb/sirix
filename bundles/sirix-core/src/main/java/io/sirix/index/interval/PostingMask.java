package io.sirix.index.interval;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.roaringbitmap.RelativeRangeConsumer;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.jspecify.annotations.Nullable;

/** Immutable companion posting mask: safe to share between read transactions of one revision. */
public final class PostingMask {
  private static final PostingMask EMPTY = new PostingMask(new long[0]);
  private final long[] words;

  private PostingMask(final long[] words) {
    this.words = words;
  }

  public static PostingMask from(final @Nullable NodeReferences references) {
    if (references == null || !references.hasNodeKeys()) {
      return EMPTY;
    }
    final long[] words = new long[1024];
    final long[] keys = references.sortedKeysOrNull();
    if (keys != null) {
      for (final long key : keys) {
        if ((key >>> 16) != 0) {
          throw new IllegalArgumentException("Posting mask references must fit the unsigned-16-bit domain");
        }
        words[(int) (key >>> 6)] |= 1L << key;
      }
      return new PostingMask(words);
    }
    // Walk the bitmap's containers word-wise. A char iterator would do the same work per key through
    // a second iterator implementation, which costs the JIT its monomorphic dispatch elsewhere.
    final Roaring64Bitmap bitmap = references.getNodeKeys();
    if (bitmap.rankLong(0xFFFFL) != bitmap.getLongCardinality()) {
      throw new IllegalArgumentException("Posting mask references must fit the unsigned-16-bit domain");
    }
    bitmap.forAllInRange(0L, 1 << 16, new WordFiller(words));
    return new PostingMask(words);
  }

  /** Sets present positions of one chunk into a word array; absent ranges are ignored. */
  private static final class WordFiller implements RelativeRangeConsumer {
    private final long[] words;

    private WordFiller(final long[] words) {
      this.words = words;
    }

    @Override
    public void acceptPresent(final int relativePos) {
      words[relativePos >>> 6] |= 1L << relativePos;
    }

    @Override
    public void acceptAbsent(final int relativePos) {}

    @Override
    public void acceptAllPresent(final int relativeFrom, final int relativeTo) {
      fillRange(words, relativeFrom, relativeTo);
    }

    @Override
    public void acceptAllAbsent(final int relativeFrom, final int relativeTo) {}
  }

  /** Set bits {@code [from, to)} of a chunk-sized word array. */
  public static void fillRange(final long[] words, final int from, final int to) {
    if (from >= to) {
      return;
    }
    final int firstWord = from >>> 6;
    final int lastWord = (to - 1) >>> 6;
    for (int word = firstWord; word <= lastWord; word++) {
      final long low = word == firstWord
          ? -1L << (from & 63)
          : -1L;
      final long high = word == lastWord
          ? -1L >>> (-to & 63)
          : -1L;
      words[word] |= low & high;
    }
  }

  public boolean isEmpty() {
    return words.length == 0;
  }

  public long word(final int index) {
    if (index < 0 || index >= 1024) {
      throw new IndexOutOfBoundsException(index);
    }
    return words.length == 0
        ? 0
        : words[index];
  }

  int cacheWeight() {
    return 128 + words.length * Long.BYTES;
  }
}
