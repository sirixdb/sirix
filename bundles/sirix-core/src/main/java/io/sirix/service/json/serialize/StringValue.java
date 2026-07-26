package io.sirix.service.json.serialize;

public final class StringValue {

  private static final String[] ESCAPE_TABLE = new String[128];

  static {
    ESCAPE_TABLE['"'] = "\\\"";
    ESCAPE_TABLE['\\'] = "\\\\";
    ESCAPE_TABLE['\b'] = "\\b";
    ESCAPE_TABLE['\f'] = "\\f";
    ESCAPE_TABLE['\n'] = "\\n";
    ESCAPE_TABLE['\r'] = "\\r";
    ESCAPE_TABLE['\t'] = "\\t";
    ESCAPE_TABLE['/'] = "\\/";
  }

  private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

  public static String escape(final String value) {
    final int len = value.length();
    // Fast path: most strings need no escaping — return the SAME instance, zero allocation
    // (this sits on the per-string-value serialization hot path).
    int first = 0;
    while (first < len && !needsEscape(value.charAt(first))) {
      first++;
    }
    if (first == len) {
      return value;
    }

    final StringBuilder sb = new StringBuilder(len + (len >> 3));
    sb.append(value, 0, first);
    for (int i = first; i < len; i++) {
      final char ch = value.charAt(i);
      if (ch < 128) {
        final String replacement = ESCAPE_TABLE[ch];
        if (replacement != null) {
          sb.append(replacement);
        } else if (ch <= '\u001F') {
          appendUnicodeEscape(sb, ch);
        } else {
          sb.append(ch);
        }
      } else if ((ch >= '\u007F' && ch <= '\u009F') || (ch >= '\u2000' && ch <= '\u20FF')) {
        appendUnicodeEscape(sb, ch);
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }

  private static boolean needsEscape(final char ch) {
    if (ch < 128) {
      return ESCAPE_TABLE[ch] != null || ch <= '\u001F';
    }
    return (ch >= '\u007F' && ch <= '\u009F') || (ch >= '\u2000' && ch <= '\u20FF');
  }

  private static void appendUnicodeEscape(final StringBuilder sb, final char ch) {
    sb.append("\\u");
    sb.append(HEX_DIGITS[(ch >> 12) & 0xF]);
    sb.append(HEX_DIGITS[(ch >> 8) & 0xF]);
    sb.append(HEX_DIGITS[(ch >> 4) & 0xF]);
    sb.append(HEX_DIGITS[ch & 0xF]);
  }
}
