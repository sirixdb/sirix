package io.sirix.query.shell;

import java.io.IOException;
import java.io.Writer;
import java.util.Objects;

/**
 * Colours serialized JSON/XML without parsing, buffering, or evaluating query items. Fixed scratch
 * buffers avoid per-value allocation, including strings larger than memory. Closing never closes
 * the terminal. Call finish after a result (flush alone must preserve tokens across serializer
 * flushes).
 */
public final class ShellResultWriter extends Writer {
  private final Writer out;
  private final ShellPalette palette;
  private final boolean diff;
  private final char[] buffer = new char[16384];
  private final char[] key = new char[16];
  private final char[] containers = new char[256];
  private int used;
  private int keyLength;
  private int depth;
  private char quote;
  private boolean escaped;
  private boolean pendingKey;
  private boolean expectKey;
  private boolean xmlTag;
  private boolean number;
  private boolean pendingWord;

  public ShellResultWriter(final Writer out, final ShellPalette palette, final boolean diff) {
    this.out = Objects.requireNonNull(out);
    this.palette = Objects.requireNonNull(palette);
    this.diff = diff;
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
      else if (c == '\\' && !xmlTag)
        escaped = true;
      else if (c == quote) {
        quote = 0;
        if (pendingKey)
          emitKey(true);
        emit(palette.reset());
        if (xmlTag)
          emit(palette.blue());
      }
      return;
    }
    if (number && !(Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '+')) {
      number = false;
      emit(palette.reset());
    }
    if (c == '"' || (xmlTag && c == '\'')) {
      quote = c;
      if (expectKey && !xmlTag) {
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
    if (c == '<') {
      xmlTag = true;
      emit(palette.blue());
    } else if (!xmlTag) {
      if (c == '{' || c == '[') {
        if (depth < containers.length)
          containers[depth] = c;
        depth++;
        expectKey = c == '{';
      } else if (c == '}' || c == ']') {
        if (depth > 0)
          depth--;
        expectKey = false;
      } else if (c == ',')
        expectKey = depth > 0 && depth <= containers.length && containers[depth - 1] == '{';
      else if (!number && (Character.isDigit(c) || c == '-')) {
        number = true;
        emit(palette.orange());
      }
    }
    if (diff && !xmlTag && depth == 0 && Character.isUnicodeIdentifierStart(c)) {
      pendingWord = true;
      keyLength = 1;
      key[0] = c;
      return;
    }
    put(c);
    if (xmlTag && c == '>') {
      xmlTag = false;
      emit(palette.reset());
    }
  }

  private void emitKey(final boolean complete) throws IOException {
    String colour = palette.blue();
    if (diff && complete) {
      if (matches("\"insert\""))
        colour = palette.green();
      else if (matches("\"update\""))
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
    if (used != 0) {
      out.write(buffer, 0, used);
      used = 0;
    }
  }

  public void finish() throws IOException {
    if (pendingWord)
      emitWord();
    if (pendingKey)
      emitKey(false);
    if (palette.enabled() && (quote != 0 || number || xmlTag))
      emit(palette.reset());
    quote = 0;
    number = false;
    escaped = false;
    xmlTag = false;
    expectKey = false;
    depth = 0;
    flush();
  }

  @Override
  public void flush() throws IOException {
    drain();
    out.flush();
  }

  @Override
  public void close() throws IOException {
    finish();
  }
}
