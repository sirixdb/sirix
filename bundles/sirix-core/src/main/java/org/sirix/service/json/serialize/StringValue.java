package org.sirix.service.json.serialize;

import java.util.HashMap;

public final class StringValue {
  public static String escape(final String value) {

    var asciiCharacters = new HashMap<Character, String>() {
      {
        put('"', "\\\"");
        put('\\', "\\\\");
        put('\b', "\\b");
        put('\f', "\\f");
        put('\n', "\\n");
        put('\r', "\\r");
        put('\t', "\\t");
        put('/', "\\/");
      }
    };

    final StringBuilder sb = new StringBuilder();
    final int len = value.length();

    for (int i = 0; i < len; i++) {
      char ch = value.charAt(i);
      sb.append(asciiCharacters.computeIfAbsent(ch, StringValue::escapeUnicode));
    }
    return sb.toString();
  }

  private static String escapeUnicode(char ch) {
    StringBuilder sb = new StringBuilder();
    //Reference: http://www.unicode.org/versions/Unicode5.1.0/
    if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F') || (ch >= '\u2000' && ch <= '\u20FF')) {
      String ss = Integer.toHexString(ch);
      sb.append("\\u");
      sb.append("0".repeat(4 - ss.length()));
      sb.append(ss.toUpperCase());
    } else {
      sb.append(ch);
    }
    return sb.toString();
  }
}
