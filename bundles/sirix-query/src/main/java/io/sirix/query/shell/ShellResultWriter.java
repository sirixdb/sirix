package io.sirix.query.shell;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Colours serialized JSON/XML without building a document tree, buffering the full result, or
 * evaluating query items. Reusable scratch buffers avoid per-value allocation, including strings
 * larger than memory; the container stack grows only for a new maximum nesting depth. Closing never
 * closes the terminal. Call finish after a result (flush alone must preserve tokens across
 * serializer flushes).
 */
public final class ShellResultWriter extends Writer {
  public enum DiffMode {
    NONE, JSON, XML
  }

  private enum XmlMarkup {
    NONE, START, DECLARATION, TAG, COMMENT, PI, CDATA
  }

  private final Writer out;
  private final ShellPalette palette;
  private final DiffMode diff;
  private final char[] buffer = new char[16384];
  private final char[] key = new char[16];
  private char[] containers = new char[256];
  private int used;
  private int keyLength;
  private int depth;
  private char quote;
  private boolean escaped;
  private boolean pendingKey;
  private boolean expectKey;
  private XmlMarkup xmlMarkup = XmlMarkup.NONE;
  private int xmlDepth;
  private boolean xmlClosing;
  private char xmlPrevious;
  private char xmlBeforePrevious;
  private boolean number;
  private boolean pendingWord;
  private boolean patchItemStart = true;

  public ShellResultWriter(final Writer out, final ShellPalette palette, final DiffMode diff) {
    this.out = Objects.requireNonNull(out);
    this.palette = Objects.requireNonNull(palette);
    this.diff = Objects.requireNonNull(diff);
  }

  @Override
  public void write(final char[] chars, final int offset, final int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, chars.length);
    if (!palette.enabled()) {
      out.write(chars, offset, length);
      return;
    }
    for (int i = offset, end = offset + length; i < end; i++)
      accept(chars[i]);
  }

  @Override
  public void write(final String text, final int offset, final int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, text.length());
    if (!palette.enabled()) {
      out.write(text, offset, length);
      return;
    }
    for (int i = offset, end = offset + length; i < end; i++)
      accept(text.charAt(i));
  }

  @Override
  public void write(final int c) throws IOException {
    if (!palette.enabled())
      out.write(c);
    else
      accept((char) c);
  }

  private void accept(final char c) throws IOException {
    if (pendingWord) {
      if (Character.isUnicodeIdentifierPart(c) && keyLength < key.length) {
        key[keyLength++] = c;
        return;
      }
      emitWord();
    }
    if (quote != 0) {
      acceptQuoted(c);
      return;
    }
    if (number && !(Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '+')) {
      number = false;
      emit(palette.reset());
    }
    if (xmlMarkup != XmlMarkup.NONE) {
      acceptXml(c);
      return;
    }
    if (c == '<') {
      patchItemStart = false;
      xmlMarkup = XmlMarkup.START;
      xmlClosing = false;
      xmlPrevious = '<';
      xmlBeforePrevious = 0;
      emit(palette.blue());
      put(c);
      return;
    }
    if (xmlDepth > 0) {
      put(c);
      return;
    }
    acceptValue(c);
  }

  private void acceptQuoted(final char c) throws IOException {
    if (pendingKey) {
      if (keyLength == key.length)
        emitKey(false);
      else
        key[keyLength++] = c;
    }
    if (!pendingKey)
      put(c);
    if (escaped)
      escaped = false;
    else if (c == '\\' && xmlMarkup == XmlMarkup.NONE && diff != DiffMode.XML)
      escaped = true;
    else if (c == quote) {
      quote = 0;
      if (pendingKey)
        emitKey(true);
      emit(palette.reset());
      if (xmlMarkup == XmlMarkup.TAG)
        emit(palette.blue());
    }
  }

  private void acceptValue(final char c) throws IOException {
    final boolean itemStart = patchItemStart;
    if (!Character.isWhitespace(c))
      patchItemStart = c == '(' || c == ',' || c == ';';
    if (c == '"' || (diff == DiffMode.XML && c == '\'')) {
      quote = c;
      if (expectKey && diff != DiffMode.XML) {
        pendingKey = true;
        keyLength = 1;
        key[0] = c;
        expectKey = false;
      } else {
        emit(palette.green());
        put(c);
      }
      return;
    }
    updateJsonState(c);
    if (!number && (diff != DiffMode.XML || itemStart) && (Character.isDigit(c) || c == '-')) {
      number = true;
      emit(palette.orange());
    }
    if (diff == DiffMode.XML && itemStart && depth == 0 && Character.isUnicodeIdentifierStart(c)) {
      pendingWord = true;
      keyLength = 1;
      key[0] = c;
      return;
    }
    put(c);
  }

  private void updateJsonState(final char c) {
    if (c == '{' || c == '[') {
      if (depth == containers.length)
        containers = Arrays.copyOf(containers, Math.multiplyExact(containers.length, 2));
      containers[depth++] = c;
      expectKey = c == '{';
    } else if (c == '}' || c == ']') {
      if (depth > 0)
        depth--;
      expectKey = false;
    } else if (c == ',')
      expectKey = depth > 0 && containers[depth - 1] == '{';
  }

  private void acceptXml(final char c) throws IOException {
    if (xmlMarkup == XmlMarkup.START) {
      xmlMarkup = switch (c) {
        case '?' -> XmlMarkup.PI;
        case '!' -> XmlMarkup.DECLARATION;
        default -> XmlMarkup.TAG;
      };
      xmlClosing = c == '/';
    } else if (xmlMarkup == XmlMarkup.DECLARATION)
      xmlMarkup = c == '['
          ? XmlMarkup.CDATA
          : XmlMarkup.COMMENT;
    if (xmlMarkup == XmlMarkup.TAG && (c == '"' || c == '\'')) {
      quote = c;
      emit(palette.green());
    }
    put(c);
    if (markupEnded(c))
      closeXmlMarkup();
    xmlBeforePrevious = xmlPrevious;
    xmlPrevious = c;
  }

  private boolean markupEnded(final char c) {
    return c == '>' && switch (xmlMarkup) {
      case TAG -> true;
      case COMMENT -> xmlPrevious == '-' && xmlBeforePrevious == '-';
      case PI -> xmlPrevious == '?';
      case CDATA -> xmlPrevious == ']' && xmlBeforePrevious == ']';
      default -> false;
    };
  }

  private void closeXmlMarkup() throws IOException {
    if (xmlMarkup == XmlMarkup.TAG) {
      if (xmlClosing) {
        if (xmlDepth > 0)
          xmlDepth--;
      } else if (xmlPrevious != '/')
        xmlDepth++;
    }
    xmlMarkup = XmlMarkup.NONE;
    emit(palette.reset());
  }

  private void emitKey(final boolean complete) throws IOException {
    String colour = palette.blue();
    if (diff != DiffMode.NONE && complete) {
      if (matches("\"insert\""))
        colour = palette.green();
      else if (matches("\"update\"") || matches("\"replace\""))
        colour = palette.orange();
      else if (matches("\"delete\""))
        colour = palette.red();
    }
    emit(colour);
    for (int i = 0; i < keyLength; i++)
      put(key[i]);
    pendingKey = false;
    keyLength = 0;
  }

  private void emitWord() throws IOException {
    final String colour = matches("insert") || matches("append")
        ? palette.green()
        : matches("replace") || matches("rename")
            ? palette.orange()
            : matches("delete")
                ? palette.red()
                : "";
    emit(colour);
    for (int i = 0; i < keyLength; i++)
      put(key[i]);
    if (!colour.isEmpty())
      emit(palette.reset());
    pendingWord = false;
    keyLength = 0;
  }

  private boolean matches(final String text) {
    if (keyLength != text.length())
      return false;
    for (int i = 0; i < keyLength; i++)
      if (key[i] != text.charAt(i))
        return false;
    return true;
  }

  private void emit(final String text) throws IOException {
    for (int i = 0; i < text.length(); i++)
      put(text.charAt(i));
  }

  private void put(final char c) throws IOException {
    if (used == buffer.length)
      drain();
    buffer[used++] = c;
  }

  private void drain() throws IOException {
    out.write(buffer, 0, used);
    used = 0;
  }

  public void finish() throws IOException {
    if (pendingWord)
      emitWord();
    if (pendingKey)
      emitKey(false);
    if (palette.enabled() && (quote != 0 || number || xmlMarkup != XmlMarkup.NONE))
      emit(palette.reset());
    quote = 0;
    number = false;
    escaped = false;
    xmlMarkup = XmlMarkup.NONE;
    xmlDepth = 0;
    expectKey = false;
    depth = 0;
    patchItemStart = true;
    flush();
  }

  @Override
  public void flush() throws IOException {
    if (used != 0)
      drain();
    out.flush();
  }

  @Override
  public void close() throws IOException {
    finish();
  }
}
