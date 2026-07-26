package io.sirix.utils;

import io.brackit.query.atomic.QNm;

import javax.xml.namespace.QName;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import static java.util.Objects.requireNonNull;

/**
 * This class provides convenience operations for XML-specific character operations.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class XMLToken {
  /** Hidden constructor. */
  private XMLToken() {
    throw new AssertionError("May never be instantiated!");
  }

  /**
   * Checks if the specified character is a valid XML character.
   *
   * @param ch the letter to be checked
   * @return result of comparison
   */
  public static boolean valid(final int ch) {
    return ch >= 0x20 && ch <= 0xD7FF || ch == 0xA || ch == 0x9 || ch == 0xD || ch >= 0xE000 && ch <= 0xFFFD
        || ch >= 0x10000 && ch <= 0x10ffff;
  }

  /**
   * Checks if the specified character is a name start character, as required e.g. by QName and
   * NCName.
   *
   * @param ch character
   * @return result of check
   */
  public static boolean isNCStartChar(final int ch) {
    return ch < 0x80
        ? ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch == '_'
        : ch < 0x300
            ? ch >= 0xC0 && ch != 0xD7 && ch != 0xF7
            : ch >= 0x370 && ch <= 0x37D || ch >= 0x37F && ch <= 0x1FFF || ch >= 0x200C && ch <= 0x200D
                || ch >= 0x2070 && ch <= 0x218F || ch >= 0x2C00 && ch <= 0x2EFF || ch >= 0x3001 && ch <= 0xD7FF
                || ch >= 0xF900 && ch <= 0xFDCF || ch >= 0xFDF0 && ch <= 0xFFFD || ch >= 0x10000 && ch <= 0xEFFFF;
  }

  /**
   * Checks if the specified character is an XML letter.
   *
   * @param ch character
   * @return result of check
   */
  public static boolean isNCChar(final int ch) {
    return isNCStartChar(ch) || (ch < 0x100
        ? digit(ch) || ch == '-' || ch == '.' || ch == 0xB7
        : ch >= 0x300 && ch <= 0x36F || ch == 0x203F || ch == 0x2040);
  }

  /**
   * Checks if the specified character is an XML first-letter.
   *
   * @param ch the letter to be checked
   * @return result of comparison
   */
  public static boolean isStartChar(final int ch) {
    return isNCStartChar(ch) || ch == ':';
  }

  /**
   * Checks if the specified character is an XML letter.
   *
   * @param ch the letter to be checked
   * @return result of comparison
   */
  public static boolean isChar(final int ch) {
    return isNCChar(ch) || ch == ':';
  }

  /**
   * Checks if the specified token is a valid NCName.
   *
   * @param v value to be checked
   * @return result of check
   */
  public static boolean isNCName(final byte[] v) {
    final int l = v.length;
    return l != 0 && ncName(v, 0) == l;
  }

  /**
   * Checks if the specified token is a valid name.
   *
   * @param v value to be checked
   * @return result of check
   */
  public static boolean isName(final byte[] v) {
    final int l = v.length;
    for (int i = 0; i < l; i += cl(v, i)) {
      final int c = cp(v, i);
      if (i == 0
          ? !isStartChar(c)
          : !isChar(c))
        return false;
    }
    return l != 0;
  }

  /**
   * Checks if the specified token is a valid NMToken.
   *
   * @param v value to be checked
   * @return result of check
   */
  public static boolean isNMToken(final byte[] v) {
    final int l = v.length;
    for (int i = 0; i < l; i += cl(v, i))
      if (!isChar(cp(v, i)))
        return false;
    return l != 0;
  }

  /**
   * Checks if the specified token is a valid QName.
   *
   * @param val value to be checked
   * @return result of check
   */
  public static boolean isQName(final byte[] val) {
    final int l = val.length;
    if (l == 0)
      return false;
    final int i = ncName(val, 0);
    if (i == l)
      return true;
    if (i == 0 || val[i] != ':')
      return false;
    final int j = ncName(val, i + 1);
    if (j == i + 1 || j != l)
      return false;
    return true;
  }

  /**
   * Checks if the specified token is a valid {@link QName}.
   *
   * @param name {@link QName} reference to check
   * @return {@code true} if it's valid, {@code false} otherwise
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static boolean isValidQName(final QNm name) {
    final String localPart = requireNonNull(name).getLocalName();
    if (!localPart.isEmpty() && !isNCName(localPart.getBytes())) {
      return false;
    }
    final String prefix = name.getPrefix();
    if (!prefix.isEmpty() && !isNCName(prefix.getBytes())) {
      return false;
    }
    // if (!isUrl(name.getNamespaceURI())) {
    // return false;
    // }
    return true;
  }

  /**
   * Determines if the provided {@code namespaceURI} is a valid {@code URI}.
   *
   * @param namespaceURI the {@code URI} to check
   * @return {@code true} if {@code namespaceURI} is valid, {@code false} otherwise
   */
  public static boolean isUrl(final String namespaceURI) {
    // NamespaceURI is never null.
    try {
      new URL(namespaceURI).toURI();
    } catch (final MalformedURLException | URISyntaxException e) {
      return false;
    }
    return true;
  }

  /**
   * Returns the codepoint (unicode value) of the specified token, starting at the specified position.
   * Returns a unicode replacement character for invalid values.
   *
   * @param token token
   * @param pos character position
   * @return current character
   */
  public static int cp(final byte[] token, final int pos) {
    // 0xxxxxxx
    final byte v = token[pos];
    if ((v & 0xFF) < 192)
      return v & 0xFF;
    // number of bytes to be read
    final int vl = cl(v);
    if (pos + vl > token.length)
      return 0xFFFD;
    // 110xxxxx 10xxxxxx
    if (vl == 2)
      return (v & 0x1F) << 6 | token[pos + 1] & 0x3F;
    // 1110xxxx 10xxxxxx 10xxxxxx
    if (vl == 3)
      return (v & 0x0F) << 12 | (token[pos + 1] & 0x3F) << 6 | token[pos + 2] & 0x3F;
    // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
    return (v & 0x07) << 18 | (token[pos + 1] & 0x3F) << 12 | (token[pos + 2] & 0x3F) << 6 | token[pos + 3] & 0x3F;
  }

  /**
   * Checks the specified token as an NCName.
   *
   * @param v value to be checked
   * @param p start position
   * @return end position
   */
  private static int ncName(final byte[] v, final int p) {
    final int l = v.length;
    for (int i = p; i < l; i += cl(v, i)) {
      final int c = cp(v, i);
      if (i == p
          ? !isNCStartChar(c)
          : !isNCChar(c))
        return i;
    }
    return l;
  }

  /**
   * Returns the codepoint length of the specified byte.
   *
   * @param first first character byte
   * @return character length
   */
  public static int cl(final byte first) {
    return first >= 0
        ? 1
        : CHLEN[first >> 4 & 0xF];
  }

  /*** Character lengths. */
  private static final int[] CHLEN = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 4};

  /**
   * Returns the codepoint length of the specified byte.
   *
   * @param token token
   * @param pos character position
   * @return character length
   */
  public static int cl(final byte[] token, final int pos) {
    return cl(token[pos]);
  }

  /**
   * Checks if the specified character is a digit (0 - 9).
   *
   * @param ch the letter to be checked
   * @return result of comparison
   */
  public static boolean digit(final int ch) {
    return ch >= '0' && ch <= '9';
  }

  /**
   * Returns the first code point of the given value which is not allowed by the XML 1.0
   * {@code Char} production (allowed are {@code #x9 | #xA | #xD | [#x20-#xD7FF] |
   * [#xE000-#xFFFD] | [#x10000-#x10FFFF]}). Unpaired surrogates are reported as invalid, too.
   * Such characters are unrepresentable in XML 1.0, even as character references.
   *
   * @param value the value to check
   * @return the first invalid code point, or {@code -1} if all characters are valid
   * @throws NullPointerException if {@code value} is {@code null}
   */
  public static int firstInvalidXmlChar(final String value) {
    requireNonNull(value);
    for (int i = 0, length = value.length(); i < length; ) {
      final int codePoint = value.codePointAt(i);
      if (!valid(codePoint)) {
        return codePoint;
      }
      i += Character.charCount(codePoint);
    }
    return -1;
  }

  /**
   * Escape characters not allowed in attribute values.
   *
   * <p>
   * Besides the markup characters, tab, line feed and carriage return are emitted as character
   * references ({@code &#x9;}/{@code &#xA;}/{@code &#xD;}): conforming parsers normalize literal
   * whitespace in attribute values to spaces, so literal serialization would silently corrupt the
   * value on a round-trip.
   *
   * @param value the string value to escape
   * @return escaped value
   * @throws NullPointerException if {@code pValue} is {@code null}
   */
  public static String escapeAttribute(final String value) {
    requireNonNull(value);
    final StringBuilder escape = new StringBuilder();
    for (final char i : value.toCharArray()) {
      switch (i) {
        case '&':
          escape.append("&amp;");
          break;
        case '<':
          escape.append("&lt;");
          break;
        case '>':
          escape.append("&gt;");
          break;
        case '"':
          escape.append("&quot;");
          break;
        case '\'':
          escape.append("&apos;");
          break;
        case '\t':
          escape.append("&#x9;");
          break;
        case '\n':
          escape.append("&#xA;");
          break;
        case '\r':
          escape.append("&#xD;");
          break;
        default:
          escape.append(i);
      }
    }
    return escape.toString();
  }

  /**
   * Escape characters not allowed text content.
   *
   * <p>
   * Carriage return is emitted as a character reference ({@code &#xD;}): conforming parsers
   * normalize literal {@code CR}/{@code CRLF} line ends to a single {@code LF}, so literal
   * serialization would silently corrupt the value on a round-trip. Tab and line feed are fine
   * literal in content.
   *
   * @param value the string value to escape
   * @return escaped value
   * @throws NullPointerException if {@code value} is {@code null}
   */
  public static String escapeContent(final String value) {
    requireNonNull(value);
    final StringBuilder escape = new StringBuilder();
    for (final char i : value.toCharArray()) {
      switch (i) {
        case '&':
          escape.append("&amp;");
          break;
        case '<':
          escape.append("&lt;");
          break;
        case '>':
          escape.append("&gt;");
          break;
        case '\r':
          escape.append("&#xD;");
          break;
        default:
          escape.append(i);
      }
    }
    return escape.toString();
  }
}
