package io.sirix.query.function.jn;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;

/** Returns the number of bytes in a string's UTF-8 representation without materialising it. */
public final class Utf8Length extends AbstractFunction {

  public static final QNm UTF8_LENGTH = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "utf8-length");

  public Utf8Length(final Signature signature) {
    super(UTF8_LENGTH, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext staticContext, final QueryContext queryContext, final Sequence[] args) {
    if (args[0] == null) {
      return new Int64(0L);
    }
    final String value = ((Atomic) args[0]).stringValue();
    long bytes = 0L;
    for (int i = 0, length = value.length(); i < length; i++) {
      final char current = value.charAt(i);
      if (current <= 0x7F) {
        bytes++;
      } else if (current <= 0x7FF) {
        bytes += 2L;
      } else if (Character.isHighSurrogate(current) && i + 1 < length
          && Character.isLowSurrogate(value.charAt(i + 1))) {
        bytes += 4L;
        i++;
      } else if (Character.isSurrogate(current)) {
        // StandardCharsets.UTF_8 replaces an isolated UTF-16 surrogate with the one-byte '?'.
        bytes++;
      } else {
        bytes += 3L;
      }
    }
    return new Int64(bytes);
  }
}
