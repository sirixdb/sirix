/*
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.service.json.shredder;

/**
 * A byte-level state machine that tracks JSON structure without materialising anything.
 *
 * <p>It is the shared core of {@link JsonPartitioner} (which uses it to find record boundaries) and
 * {@link JsonPartitionInputStream} (which uses it to splice record separators back in). Both must
 * agree byte-for-byte on where a top-level value starts, so the logic lives here once rather than
 * twice.
 *
 * <p>The scanner keeps three primitives — a nesting depth and two flags — and branches once per
 * byte. It never allocates, so a pass over the input costs a load, a compare and a branch per byte
 * and stays memory-bandwidth bound.
 *
 * <h2>Why byte-level scanning is UTF-8 safe</h2>
 * Every byte of a multi-byte UTF-8 sequence has its high bit set, so none of them can collide with
 * the ASCII structural characters this scanner looks for ({@code " \ { } [ ] ,} and whitespace).
 * Scanning raw bytes therefore needs no decoding and cannot split a code point.
 *
 * <p>Instances are mutable and <strong>not</strong> thread-safe; each scanning thread owns one.
 *
 * @author Johannes Lichtenberger
 */
final class JsonStructureScanner {

  /** Container nesting depth: incremented by {@code {} and {@code [}, decremented by their closers. */
  private int depth;

  /** Whether the cursor is inside a string literal (the opening quote has been consumed). */
  private boolean inString;

  /**
   * The quote character that opened the current string literal. Gson's reader is lenient and accepts
   * single-quoted strings, so a scanner that only knew {@code "} would read the separators inside
   * {@code 'a,b'} as real structure and cut a partition through the middle of the literal.
   */
  private byte quoteChar;

  /** Whether the previous byte was a backslash inside a string literal. */
  private boolean escaped;

  /** Whether the scanner is currently inside a top-level value. */
  private boolean inValue;

  /** Depth before the last {@link #step} was applied. */
  private int depthBefore;

  /** Whether the last byte was structural, i.e. outside a string literal. */
  private boolean structural;

  /** Whether the last byte was part of a value rather than inter-value whitespace. */
  private boolean content;

  /** One past the offset of the last content byte seen at top level; {@code -1} until the first. */
  private long lastTopLevelValueEnd = -1L;

  /**
   * Snapshot of {@link #lastTopLevelValueEnd} taken when the current top-level value began — that is,
   * where the <em>preceding</em> top-level value ended. This is the cut point a partitioner wants:
   * reading {@link #lastTopLevelValueEnd} at that moment would already include the new value's first
   * byte whenever that value is a bare scalar.
   */
  private long previousTopLevelValueEnd = -1L;

  /** Whether the last {@link #step} began a new top-level value. */
  private boolean startedTopLevelValue;

  /**
   * Apply one byte.
   *
   * @param b      the byte
   * @param offset the byte's absolute offset in the stream being scanned
   */
  void step(final byte b, final long offset) {
    depthBefore = depth;
    structural = !inString;

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (b == '\\') {
        escaped = true;
      } else if (b == quoteChar) {
        inString = false;
      }
    } else {
      switch (b) {
        case '"', '\'' -> {
          inString = true;
          quoteChar = b;
        }
        case '{', '[' -> depth++;
        case '}', ']' -> depth--;
        default -> {
          // Neither opens nor closes a container.
        }
      }
    }

    content = !(structural && isWhitespace(b));
    startedTopLevelValue = false;

    if (depth == 0 && !content) {
      // Whitespace at the top level terminates a bare scalar (`123 456`); containers are terminated
      // by their closing bracket below.
      inValue = false;
      return;
    }

    final boolean opensContainerAtTopLevel = structural && depthBefore == 0 && depth == 1;
    if (content && (!inValue || opensContainerAtTopLevel)) {
      inValue = true;
      startedTopLevelValue = true;
      // Snapshot before the update below, which would otherwise fold this value's own first byte
      // into the preceding value's end for bare scalars.
      previousTopLevelValueEnd = lastTopLevelValueEnd;
    }

    if (content && depth == 0) {
      lastTopLevelValueEnd = offset + 1;
    }

    if (structural && depthBefore == 1 && depth == 0) {
      // A container closed back to the top level: the value is complete even without whitespace
      // after it, so `{"a":1}{"b":2}` splits correctly.
      inValue = false;
    } else if (depth == 0 && !structural && !inString && b == quoteChar) {
      // A string closed at the top level. Like a container close this completes the value, and it
      // must be treated as such: a closing quote is neither whitespace nor a depth change, so
      // without this `"a""b"` would read as one value and every record after the first would be
      // silently dropped.
      inValue = false;
    }
  }

  /** Whether the byte just applied began a new top-level value. */
  boolean startedTopLevelValue() {
    return startedTopLevelValue;
  }

  /** Whether the byte just applied closed a container back to the top level. */
  boolean closedTopLevelValue() {
    return structural && depthBefore == 1 && depth == 0;
  }

  /** Whether the byte just applied was structural, i.e. outside a string literal. */
  boolean isStructural() {
    return structural;
  }

  /** Whether the byte just applied belonged to a value rather than to inter-value whitespace. */
  boolean isContent() {
    return content;
  }

  /** The nesting depth after the byte just applied. */
  int depth() {
    return depth;
  }

  /** The nesting depth before the byte just applied. */
  int depthBefore() {
    return depthBefore;
  }

  /** One past the offset of the last content byte seen at top level, or {@code -1} if there was none. */
  long lastTopLevelValueEnd() {
    return lastTopLevelValueEnd;
  }

  /**
   * Where the top-level value preceding the current one ended, or {@code -1} if the current value is
   * the first. Only meaningful right after a {@link #step} for which {@link #startedTopLevelValue()}
   * holds.
   */
  long previousTopLevelValueEnd() {
    return previousTopLevelValueEnd;
  }

  /** Whether the scanner is currently inside a string literal. */
  boolean inString() {
    return inString;
  }

  /**
   * The quote character that opened the current string literal — {@code "} or, in lenient input,
   * {@code '}. Only meaningful while {@link #inString()} holds.
   */
  byte quoteChar() {
    return quoteChar;
  }

  static boolean isWhitespace(final byte b) {
    return b == ' ' || b == '\n' || b == '\r' || b == '\t';
  }
}
