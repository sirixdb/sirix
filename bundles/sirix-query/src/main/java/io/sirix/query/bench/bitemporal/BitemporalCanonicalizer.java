package io.sirix.query.bench.bitemporal;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Strict representation-only canonicalization for SH1 result rows. */
public final class BitemporalCanonicalizer {

  private BitemporalCanonicalizer() {
    throw new AssertionError("no instances");
  }

  public record Result(long rows, String sha256) {
  }

  public static Result write(final BitemporalQueries.Query query, final String serialized, final Path output)
      throws IOException, NoSuchAlgorithmException {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    final long[][] priorKey = new long[1][];
    long rows = 0;
    try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8);
        JsonReader reader = new JsonReader(new StringReader(serialized))) {
      reader.setStrictness(Strictness.LENIENT);
      while (reader.peek() != JsonToken.END_DOCUMENT) {
        final JsonElement item = JsonParser.parseReader(reader);
        if (!item.isJsonObject()) {
          throw new IllegalArgumentException("Q" + query.index() + " emitted a non-object row: " + item);
        }
        final JsonObject object = item.getAsJsonObject();
        if (object.size() != query.columns().size()) {
          throw new IllegalArgumentException(
              "Q" + query.index() + " schema width " + object.size() + " != " + query.columns().size());
        }
        final long[] values = new long[query.columns().size()];
        final StringBuilder line = new StringBuilder(values.length * 18);
        for (int column = 0; column < values.length; column++) {
          final String name = query.columns().get(column);
          if (!object.has(name) || object.get(name).isJsonNull()) {
            throw new IllegalArgumentException("Q" + query.index() + " missing/null column " + name);
          }
          values[column] = exactLong(query.index(), name, object.get(name));
          if (column > 0) {
            line.append('\t');
          }
          line.append(values[column]);
        }
        validateKey(query, priorKey[0], values);
        if (query.keyColumns() > 0) {
          priorKey[0] = values.clone();
        }
        line.append('\n');
        final String canonical = line.toString();
        writer.write(canonical);
        digest.update(canonical.getBytes(StandardCharsets.UTF_8));
        rows++;
      }
    }
    if (query.index() == 3 && rows != 1) {
      throw new IllegalArgumentException("Q3 must emit exactly one non-null aggregate row, got " + rows);
    }
    return new Result(rows, HexFormat.of().formatHex(digest.digest()));
  }

  private static long exactLong(final int query, final String column, final JsonElement element) {
    if (!element.isJsonPrimitive()) {
      throw new IllegalArgumentException("Q" + query + ' ' + column + " is not scalar: " + element);
    }
    final JsonPrimitive primitive = element.getAsJsonPrimitive();
    if (!primitive.isNumber()) {
      throw new IllegalArgumentException("Q" + query + ' ' + column + " is not numeric: " + element);
    }
    final BigInteger integer;
    try {
      integer = new BigDecimal(primitive.getAsString()).toBigIntegerExact();
    } catch (final ArithmeticException | NumberFormatException e) {
      throw new IllegalArgumentException("Q" + query + ' ' + column + " is fractional/invalid: " + element, e);
    }
    if (integer.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
        || integer.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
      throw new IllegalArgumentException("Q" + query + ' ' + column + " overflows signed 64-bit: " + integer);
    }
    return integer.longValue();
  }

  private static void validateKey(final BitemporalQueries.Query query, final long[] prior, final long[] current) {
    if (query.keyColumns() == 0 || prior == null) {
      return;
    }
    for (int column = 0; column < query.keyColumns(); column++) {
      final int comparison = Long.compare(prior[column], current[column]);
      if (comparison < 0) {
        return;
      }
      if (comparison > 0) {
        throw new IllegalArgumentException("Q" + query.index() + " rows are not ordered at key column " + column);
      }
    }
    throw new IllegalArgumentException("Q" + query.index() + " emitted a duplicate key");
  }
}
