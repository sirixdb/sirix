/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.service.json.JsonNumber;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Minimal strict-JSON scanner for the bulk assembler: one forward pass over a {@link Reader},
 * emitting structural events without the general-purpose tokenizer overhead the profile charged at
 * ~25% of fill time — no per-token enum peek state machine, no {@code String} materialization for
 * values (UTF-8 bytes are encoded straight from the char buffer into a reusable array), and field
 * names resolve through an intern table so each distinct name allocates exactly once.
 *
 * <p>
 * EQUIVALENCE CONTRACT (oracle-enforced: the cursor arm still parses with Gson, so every
 * differential fixture doubles as a tokenizer-equivalence test):
 * <ul>
 * <li>Escape decoding matches JSON exactly — {@code \" \\ \/ \b \f \n \r \t \/uXXXX} including
 * surrogate pairs assembled from consecutive escapes.</li>
 * <li>UTF-8 value bytes are byte-identical to {@code String.getBytes(UTF_8)} over the decoded
 * chars, INCLUDING the {@code '?'} replacement byte for unpaired surrogates.</li>
 * <li>Ordinary integers accumulate into an overflow-checked primitive while their characters are
 * scanned; decimal, exponent, overflow and malformed forms fall back verbatim to
 * {@link JsonNumber#stringToNumber(String)}. The exposed {@code Number} types and values therefore
 * remain identical.</li>
 * </ul>
 *
 * <p>
 * Single-threaded; the value/name buffers are reused between events, so callers must consume an
 * event's data before requesting the next.
 */
final class BulkJsonScanner {

  static final int EVENT_BEGIN_OBJECT = 0;
  static final int EVENT_END_OBJECT = 1;
  static final int EVENT_BEGIN_ARRAY = 2;
  static final int EVENT_END_ARRAY = 3;
  static final int EVENT_NAME = 4;
  static final int EVENT_STRING = 5;
  static final int EVENT_NUMBER = 6;
  static final int EVENT_TRUE = 7;
  static final int EVENT_FALSE = 8;
  static final int EVENT_NULL = 9;
  static final int EVENT_END_DOCUMENT = 10;

  /** No numeric event is currently exposed. */
  static final int NUMBER_TYPE_NONE = 0;
  /** The current numeric event is represented exactly by an {@code int}. */
  static final int NUMBER_TYPE_INT = 1;
  /** The current numeric event is represented exactly by a {@code long}. */
  static final int NUMBER_TYPE_LONG = 2;
  /** The current numeric event requires the ordinary {@link Number} fallback representation. */
  static final int NUMBER_TYPE_FALLBACK = 3;

  private static final int DEFAULT_BUFFER_CHARS = 1 << 16;

  /**
   * Whether field names are canonicalised from the decode buffer instead of through a String-keyed
   * map. Kill switch {@code -Dsirix.ingest.internTable=false} restores the map, and with it one
   * transient String plus its value array per key OCCURRENCE.
   */
  private static final boolean INTERN_TABLE_ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("sirix.ingest.internTable"));

  /** The default refill-buffer size, for callers that must name the shared-name-table constructor. */
  static int defaultBufferChars() {
    return DEFAULT_BUFFER_CHARS;
  }

  private final Reader in;
  private final char[] buffer;
  private int position;
  private int limit;

  /** Decoded chars of the current string/number token (escape-processed). */
  private char[] token = new char[256];
  private int tokenLength;

  /**
   * Exposed state of the CURRENT event vs scratch state of a scanned-ahead event. One-event lookahead
   * must never clobber what the caller has not yet consumed: the aliasing bug this structure exists
   * for was a peeked STRING re-encoding over the byte buffer of the string the factory was about to
   * write — consecutive array strings shifted by one, pinned by the oracle's lone-surrogate fixture.
   * {@link #next()} promotes scratch to current by SWAPPING the byte arrays, so the zero-copy
   * property survives.
   */
  private byte[] utf8 = new byte[512];
  private int utf8Length;
  private byte[] scratchUtf8 = new byte[512];
  private int scratchUtf8Length;

  /**
   * Canonical name strings looked up straight from {@link #token}, so a repeat field-name occurrence
   * allocates nothing. Shared across the import when the importer supplies one, which is what makes
   * the canonical instance global rather than per chunk.
   */
  private final NameInternTable nameInternTable;

  /**
   * Legacy lane, allocated only when {@code -Dsirix.ingest.internTable=false} selects it: a map keyed
   * by String, which must BUILD one to look one up. Kept so the lever has an off position that is the
   * previous behaviour exactly, not an approximation of it.
   */
  private final Map<String, String> nameInterns;

  private String currentName;
  private Number currentNumber;
  private String scratchName;
  private Number scratchNumber;

  /**
   * Primitive numeric state follows the same scratch/current handoff as the UTF-8 buffers. A peek may
   * populate the scratch lane, but it cannot overwrite the event the caller is still consuming.
   */
  private int currentNumberType;
  private long currentIntegralNumberValue;
  private int scratchNumberType;
  private long scratchIntegralNumberValue;

  private int peekedEvent = -1;

  BulkJsonScanner(final Reader in) {
    this(in, DEFAULT_BUFFER_CHARS, null);
  }

  /** Buffer size is a test seam: tiny buffers force refills inside tokens. */
  BulkJsonScanner(final Reader in, final int bufferChars) {
    this(in, bufferChars, null);
  }

  /**
   * @param in the character source
   * @param bufferChars refill-buffer size; a test seam, tiny buffers force refills inside tokens
   * @param sharedNames the import-wide canonical-name table, or {@code null} to own a private one
   */
  BulkJsonScanner(final Reader in, final int bufferChars, final @Nullable NameInternTable sharedNames) {
    this.in = in;
    this.buffer = new char[Math.max(16, bufferChars)];
    if (INTERN_TABLE_ENABLED) {
      this.nameInternTable = sharedNames != null
          ? sharedNames
          : new NameInternTable();
      this.nameInterns = null;
    } else {
      this.nameInternTable = null;
      this.nameInterns = new HashMap<>();
    }
  }

  int peek() throws IOException {
    if (peekedEvent < 0) {
      peekedEvent = advance();
    }
    return peekedEvent;
  }

  int next() throws IOException {
    final int event;
    if (peekedEvent >= 0) {
      event = peekedEvent;
      peekedEvent = -1;
    } else {
      event = advance();
    }
    // Promote the scanned event's scratch state to the exposed slots. Swapping the arrays keeps
    // the encoding zero-copy while guaranteeing a later peek can never touch what we return now.
    final byte[] previouslyExposed = utf8;
    utf8 = scratchUtf8;
    utf8Length = scratchUtf8Length;
    scratchUtf8 = previouslyExposed;
    currentName = scratchName;
    currentNumber = scratchNumber;
    currentNumberType = scratchNumberType;
    currentIntegralNumberValue = scratchIntegralNumberValue;
    return event;
  }

  /** Valid after {@link #next()} returned {@link #EVENT_NAME}; canonical (interned) instance. */
  String name() {
    return currentName;
  }

  /** Valid after {@link #next()} returned {@link #EVENT_NUMBER}. */
  Number number() {
    // Preserve the existing Number-shaped scanner contract, but materialize a wrapper only for a
    // caller that actually asks for it. Primitive-aware sinks can consume numberType() and
    // integralNumberValue() without allocating an Integer or Long per token.
    if (currentNumber == null) {
      if (currentNumberType == NUMBER_TYPE_INT) {
        currentNumber = Integer.valueOf((int) currentIntegralNumberValue);
      } else if (currentNumberType == NUMBER_TYPE_LONG) {
        currentNumber = Long.valueOf(currentIntegralNumberValue);
      }
    }
    return currentNumber;
  }

  /**
   * Primitive representation tag for the current numeric event. Valid after {@link #next()} returns
   * {@link #EVENT_NUMBER}; {@link #NUMBER_TYPE_FALLBACK} means callers must use {@link #number()}.
   */
  int numberType() {
    return currentNumberType;
  }

  /**
   * Exact primitive value of the current {@link #NUMBER_TYPE_INT} or {@link #NUMBER_TYPE_LONG} event.
   *
   * @throws IllegalStateException if the current event has no primitive integral representation
   */
  long integralNumberValue() {
    if (currentNumberType != NUMBER_TYPE_INT && currentNumberType != NUMBER_TYPE_LONG) {
      throw new IllegalStateException("current number has no primitive integral representation");
    }
    return currentIntegralNumberValue;
  }

  /** Valid after {@link #next()} returned {@link #EVENT_STRING}; reused buffer. */
  byte[] stringUtf8() {
    return utf8;
  }

  int stringUtf8Length() {
    return utf8Length;
  }

  // ==== scanning ==============================================================================

  private int advance() throws IOException {
    while (true) {
      final int c = nextNonWhitespace();
      switch (c) {
        case -1 -> {
          return EVENT_END_DOCUMENT;
        }
        case '{' -> {
          return EVENT_BEGIN_OBJECT;
        }
        case '}' -> {
          return EVENT_END_OBJECT;
        }
        case '[' -> {
          return EVENT_BEGIN_ARRAY;
        }
        case ']' -> {
          return EVENT_END_ARRAY;
        }
        case ',', ':' -> {
          // Structural separators carry no event; strictness (matching them to context) is the
          // parser-level concern the assembler's own stack enforces structurally.
        }
        case '"' -> {
          scanQuoted();
          if (peekNonWhitespaceIsColon()) {
            scratchName = intern(token, tokenLength);
            return EVENT_NAME;
          }
          encodeUtf8();
          return EVENT_STRING;
        }
        case 't' -> {
          expect("rue");
          return EVENT_TRUE;
        }
        case 'f' -> {
          expect("alse");
          return EVENT_FALSE;
        }
        case 'n' -> {
          expect("ull");
          return EVENT_NULL;
        }
        default -> {
          if (c == '-' || (c >= '0' && c <= '9')) {
            scanNumber((char) c);
            return EVENT_NUMBER;
          }
          throw new IOException("unexpected character '" + (char) c + "' in JSON input");
        }
      }
    }
  }

  private int nextNonWhitespace() throws IOException {
    while (true) {
      if (position == limit && !fill()) {
        return -1;
      }
      final char c = buffer[position++];
      if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
        return c;
      }
    }
  }

  /** After a closing quote: does a colon follow (name) or not (string value)? Consumes nothing. */
  private boolean peekNonWhitespaceIsColon() throws IOException {
    while (true) {
      for (int i = position; i < limit; i++) {
        final char c = buffer[i];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
          continue;
        }
        return c == ':';
      }
      // The colon (or the next value) may sit beyond the buffered window: compact + refill and
      // look again. Position is preserved because fill() compacts from `position`.
      if (!fill()) {
        return false;
      }
    }
  }

  private void scanQuoted() throws IOException {
    tokenLength = 0;
    while (true) {
      if (position == limit && !fill()) {
        throw new IOException("unterminated string");
      }
      final char c = buffer[position++];
      if (c == '"') {
        return;
      }
      if (c == '\\') {
        appendToken(readEscape());
      } else {
        appendToken(c);
      }
    }
  }

  private char readEscape() throws IOException {
    if (position == limit && !fill()) {
      throw new IOException("unterminated escape");
    }
    final char c = buffer[position++];
    return switch (c) {
      case '"' -> '"';
      case '\\' -> '\\';
      case '/' -> '/';
      case 'b' -> '\b';
      case 'f' -> '\f';
      case 'n' -> '\n';
      case 'r' -> '\r';
      case 't' -> '\t';
      case 'u' -> readUnicodeEscape();
      default -> throw new IOException("invalid escape \\" + c);
    };
  }

  private char readUnicodeEscape() throws IOException {
    int value = 0;
    for (int i = 0; i < 4; i++) {
      if (position == limit && !fill()) {
        throw new IOException("unterminated unicode escape");
      }
      final char c = buffer[position++];
      final int digit = Character.digit(c, 16);
      if (digit < 0) {
        throw new IOException("invalid unicode escape digit '" + c + "'");
      }
      value = (value << 4) | digit;
    }
    return (char) value;
  }

  private void scanNumber(final char first) throws IOException {
    tokenLength = 0;
    appendToken(first);

    // Parse the ordinary integral lane while the scanner already traverses the token. Accumulating
    // negatively is the same overflow-safe technique used by JsonNumber.stringInteger and admits
    // Long.MIN_VALUE, whose positive magnitude cannot be represented by a long. Any decimal mark,
    // exponent syntax, embedded sign, overflow, or malformed sign-only token takes the exact old
    // String/JsonNumber path below.
    final boolean negative = first == '-';
    final long overflowLimit = negative
        ? Long.MIN_VALUE
        : -Long.MAX_VALUE;
    final long multiplyLimit = overflowLimit / 10;
    long result = 0;
    int digits = 0;
    boolean primitiveIntegral = true;
    if (!negative) {
      result = -(first - '0');
      digits = 1;
    }

    while (true) {
      if (position == limit && !fill()) {
        break;
      }
      final char c = buffer[position];
      if ((c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
        appendToken(c);
        position++;
        if (primitiveIntegral && c >= '0' && c <= '9') {
          final int digit = c - '0';
          if (result < multiplyLimit) {
            primitiveIntegral = false;
          } else {
            result *= 10;
            if (result < overflowLimit + digit) {
              primitiveIntegral = false;
            } else {
              result -= digit;
              digits++;
            }
          }
        } else {
          primitiveIntegral = false;
        }
      } else {
        break;
      }
    }

    if (primitiveIntegral && digits > 0) {
      final long value = negative
          ? result
          : -result;
      scratchIntegralNumberValue = value;
      scratchNumberType = value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE
          ? NUMBER_TYPE_INT
          : NUMBER_TYPE_LONG;
      scratchNumber = null;
      return;
    }

    scratchNumber = JsonNumber.stringToNumber(new String(token, 0, tokenLength));
    scratchNumberType = NUMBER_TYPE_FALLBACK;
  }

  private void expect(final String rest) throws IOException {
    for (int i = 0; i < rest.length(); i++) {
      if (position == limit && !fill()) {
        throw new IOException("truncated literal");
      }
      if (buffer[position++] != rest.charAt(i)) {
        throw new IOException("malformed literal");
      }
    }
  }

  private void appendToken(final char c) {
    if (tokenLength == token.length) {
      token = Arrays.copyOf(token, token.length << 1);
    }
    token[tokenLength++] = c;
  }

  /** Compact unconsumed chars to the front and refill; false at end of input. */
  private boolean fill() throws IOException {
    if (position < limit) {
      System.arraycopy(buffer, position, buffer, 0, limit - position);
    }
    limit -= position;
    position = 0;
    final int read = in.read(buffer, limit, buffer.length - limit);
    if (read <= 0) {
      return false;
    }
    limit += read;
    return true;
  }

  /**
   * The canonical instance for the field name currently in {@link #token}.
   *
   * <p>
   * Both lanes keep the CANONICAL instance stable, so the downstream maps (PCR memo, name memo) see
   * the same object for every occurrence of a name and their first equality test is a pointer
   * comparison. They differ in what that costs. The char-slice table hashes and compares the decode
   * buffer in place, so a repeat occurrence allocates nothing; the legacy map is keyed by
   * {@code String} and so must BUILD one — a String plus its Latin-1 value array — merely to look one
   * up, then drop it. That was 105,000,000 transient Strings and 5.39 GB on a 1M-row load, 35 % of
   * everything the load allocated.
   * </p>
   */
  private String intern(final char[] chars, final int length) {
    if (nameInternTable != null) {
      final String canonical = nameInternTable.intern(chars, 0, length);
      if (BulkImportAllocationDiag.ENABLED) {
        // A hit here allocates NOTHING, which is the whole point; the counter still records the
        // occurrence so the before/after is measured on the same denominator.
        BulkImportAllocationDiag.recordCanonicalNameLookup(length);
      }
      return canonical;
    }
    final String candidate = new String(chars, 0, length);
    final String canonical = nameInterns.get(candidate);
    if (BulkImportAllocationDiag.ENABLED) {
      // Diagnostic only: prices the transient String and its Latin-1 byte[] that this method mints
      // per OCCURRENCE. Gated on a static final read, so the branch folds away when it is off.
      BulkImportAllocationDiag.recordNameIntern(length, canonical != null);
    }
    if (canonical != null) {
      return canonical;
    }
    nameInterns.put(candidate, candidate);
    return candidate;
  }

  /**
   * Encode {@link #token} to UTF-8, byte-identical to {@code new String(chars).getBytes(UTF_8)} —
   * including the {@code '?'} replacement for unpaired surrogates.
   */
  private void encodeUtf8() {
    final int worstCase = tokenLength * 3 + 4;
    if (scratchUtf8.length < worstCase) {
      scratchUtf8 = new byte[Integer.highestOneBit(worstCase) << 1];
    }
    final byte[] target = scratchUtf8;
    int out = 0;
    for (int i = 0; i < tokenLength; i++) {
      final char c = token[i];
      if (c < 0x80) {
        target[out++] = (byte) c;
      } else if (c < 0x800) {
        target[out++] = (byte) (0xC0 | (c >> 6));
        target[out++] = (byte) (0x80 | (c & 0x3F));
      } else if (Character.isHighSurrogate(c)) {
        if (i + 1 < tokenLength && Character.isLowSurrogate(token[i + 1])) {
          final int codePoint = Character.toCodePoint(c, token[++i]);
          target[out++] = (byte) (0xF0 | (codePoint >> 18));
          target[out++] = (byte) (0x80 | ((codePoint >> 12) & 0x3F));
          target[out++] = (byte) (0x80 | ((codePoint >> 6) & 0x3F));
          target[out++] = (byte) (0x80 | (codePoint & 0x3F));
        } else {
          target[out++] = '?';
        }
      } else if (Character.isLowSurrogate(c)) {
        target[out++] = '?';
      } else {
        target[out++] = (byte) (0xE0 | (c >> 12));
        target[out++] = (byte) (0x80 | ((c >> 6) & 0x3F));
        target[out++] = (byte) (0x80 | (c & 0x3F));
      }
    }
    scratchUtf8Length = out;
  }
}
