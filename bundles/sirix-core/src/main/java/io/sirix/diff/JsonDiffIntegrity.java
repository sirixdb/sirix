package io.sirix.diff;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Integrity metadata for the internal per-revision JSON diff sidecar.
 *
 * <p>The sidecar is a rebuildable cache, not authoritative database state. A strict schema check
 * catches malformed operations but cannot distinguish a legitimate subset from a syntactically
 * valid file that lost one or more operations. Version, count, and a SHA-256 digest over the
 * semantic {@code diffs} tree let readers reject that case and use an authoritative fallback (or
 * fail before applying a partial revision copy).</p>
 *
 * <p>The digest is fed directly from the Gson tree without materializing a second JSON string or
 * byte array. Strings are length-prefixed UTF-16 code units; arrays and objects include type tags,
 * sizes, names, values, and insertion order. Both writer and strict reader use the same canonical
 * representation, independent of insignificant JSON whitespace.</p>
 */
public final class JsonDiffIntegrity {

  public static final String FORMAT_VERSION_FIELD = "sirix-diff-format";
  public static final String OPERATION_COUNT_FIELD = "operation-count";
  public static final String OPERATIONS_DIGEST_FIELD = "operations-sha256";
  public static final int FORMAT_VERSION = 1;

  private JsonDiffIntegrity() {
    throw new AssertionError("No instances");
  }

  /** Add or replace the integrity metadata for {@code document}. */
  public static void add(final JsonObject document) {
    final JsonArray operations = operations(document);
    document.addProperty(FORMAT_VERSION_FIELD, FORMAT_VERSION);
    document.addProperty(OPERATION_COUNT_FIELD, operations.size());
    document.addProperty(OPERATIONS_DIGEST_FIELD, digest(operations));
  }

  /**
   * Validate the sidecar integrity metadata.
   *
   * @throws IllegalStateException if metadata is absent, malformed, or does not match the operation
   *         tree
   */
  public static void validate(final JsonObject document) {
    final JsonArray operations = operations(document);
    if (requiredInt(document, FORMAT_VERSION_FIELD) != FORMAT_VERSION) {
      throw new IllegalStateException("Unsupported JSON diff sidecar format");
    }
    if (requiredInt(document, OPERATION_COUNT_FIELD) != operations.size()) {
      throw new IllegalStateException("JSON diff sidecar operation count does not match its payload");
    }

    final JsonElement digestElement = document.get(OPERATIONS_DIGEST_FIELD);
    if (digestElement == null || !digestElement.isJsonPrimitive()
        || !digestElement.getAsJsonPrimitive().isString()) {
      throw new IllegalStateException("JSON diff sidecar digest is missing or malformed");
    }
    final String expected = digestElement.getAsString();
    final String actual = digest(operations);
    if (!expected.equals(actual)) {
      throw new IllegalStateException("JSON diff sidecar digest does not match its payload");
    }
  }

  private static JsonArray operations(final JsonObject document) {
    final JsonElement operations = document.get("diffs");
    if (operations == null || !operations.isJsonArray()) {
      throw new IllegalStateException("JSON diff sidecar has no diffs array");
    }
    return operations.getAsJsonArray();
  }

  private static int requiredInt(final JsonObject document, final String name) {
    final JsonElement value = document.get(name);
    if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
      throw new IllegalStateException("JSON diff sidecar field '" + name + "' is missing or malformed");
    }
    try {
      return Integer.parseInt(value.getAsString());
    } catch (final NumberFormatException e) {
      throw new IllegalStateException("JSON diff sidecar field '" + name + "' is not an integer", e);
    }
  }

  private static String digest(final JsonArray operations) {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
    update(digest, operations);
    return HexFormat.of().formatHex(digest.digest());
  }

  private static void update(final MessageDigest digest, final JsonElement element) {
    if (element == null || element.isJsonNull()) {
      digest.update((byte) 0);
      return;
    }
    if (element.isJsonArray()) {
      digest.update((byte) 1);
      final JsonArray array = element.getAsJsonArray();
      updateInt(digest, array.size());
      for (final JsonElement value : array) {
        update(digest, value);
      }
      return;
    }
    if (element.isJsonObject()) {
      digest.update((byte) 2);
      final JsonObject object = element.getAsJsonObject();
      updateInt(digest, object.size());
      for (final var entry : object.entrySet()) {
        updateString(digest, entry.getKey());
        update(digest, entry.getValue());
      }
      return;
    }

    final JsonPrimitive primitive = element.getAsJsonPrimitive();
    if (primitive.isBoolean()) {
      digest.update((byte) 3);
      digest.update(primitive.getAsBoolean()
          ? (byte) 1
          : (byte) 0);
    } else if (primitive.isNumber()) {
      digest.update((byte) 4);
      updateString(digest, primitive.getAsString());
    } else if (primitive.isString()) {
      digest.update((byte) 5);
      updateString(digest, primitive.getAsString());
    } else {
      throw new IllegalStateException("Unsupported JSON diff primitive: " + primitive);
    }
  }

  private static void updateString(final MessageDigest digest, final String value) {
    updateInt(digest, value.length());
    for (int index = 0; index < value.length(); index++) {
      final char current = value.charAt(index);
      digest.update((byte) (current >>> 8));
      digest.update((byte) current);
    }
  }

  private static void updateInt(final MessageDigest digest, final int value) {
    digest.update((byte) (value >>> 24));
    digest.update((byte) (value >>> 16));
    digest.update((byte) (value >>> 8));
    digest.update((byte) value);
  }
}
