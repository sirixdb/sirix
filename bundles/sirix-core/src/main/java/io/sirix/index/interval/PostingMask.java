package io.sirix.index.interval;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
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
    references.forEachNodeKey(key -> {
      if ((key >>> 16) != 0) {
        throw new IllegalArgumentException("Posting mask references must fit the unsigned-16-bit domain");
      }
      words[(int) (key >>> 6)] |= 1L << key;
    });
    return new PostingMask(words);
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
