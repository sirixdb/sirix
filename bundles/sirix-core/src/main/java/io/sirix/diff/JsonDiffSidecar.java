package io.sirix.diff;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.sirix.node.SirixDeweyID;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Strict single-read decoder and validator for internal JSON diff sidecars. */
public final class JsonDiffSidecar {

  private JsonDiffSidecar() {
    throw new AssertionError("No instances");
  }

  /**
   * Read and validate one immutable per-revision sidecar.
   *
   * @param path sidecar path
   * @param expectedResource resource name encoded in the sidecar
   * @param expectedOldRevision old revision encoded in the sidecar
   * @param expectedNewRevision new revision encoded in the sidecar
   * @param requireDeweyMetadata whether every operation must carry a Dewey ID and matching depth
   * @return the validated diff document
   * @throws IOException if the sidecar cannot be read
   * @throws IllegalStateException if strict JSON, identity, integrity, Unicode, or operation schema
   *         validation fails
   */
  public static JsonObject read(final Path path, final String expectedResource, final int expectedOldRevision,
      final int expectedNewRevision, final boolean requireDeweyMetadata) throws IOException {
    final JsonElement root;
    try (final var bufferedReader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
        final var reader = new JsonReader(bufferedReader)) {
      reader.setStrictness(Strictness.STRICT);
      if (reader.peek() == JsonToken.END_DOCUMENT) {
        throw new IllegalStateException("JSON diff sidecar is empty");
      }
      root = JsonParser.parseReader(reader);
      if (reader.peek() != JsonToken.END_DOCUMENT) {
        throw new IllegalStateException("JSON diff sidecar has trailing content");
      }
    } catch (final JsonParseException e) {
      throw new IllegalStateException("JSON diff sidecar is not strict JSON", e);
    }

    if (!root.isJsonObject()) {
      throw new IllegalStateException("JSON diff sidecar root is not an object");
    }
    final JsonObject document = root.getAsJsonObject();
    validateUnicodeScalarValues(document);
    validateIdentity(document, expectedResource, expectedOldRevision, expectedNewRevision);
    JsonDiffIntegrity.validate(document);
    for (final JsonElement operation : document.getAsJsonArray("diffs")) {
      validateOperation(operation, requireDeweyMetadata);
    }
    return document;
  }

  private static void validateIdentity(final JsonObject document, final String expectedResource,
      final int expectedOldRevision, final int expectedNewRevision) {
    if (!hasString(document, "database") || !hasStringValue(document, "resource", expectedResource)
        || !hasIntValue(document, "old-revision", expectedOldRevision)
        || !hasIntValue(document, "new-revision", expectedNewRevision)) {
      throw new IllegalStateException("JSON diff sidecar identity does not match its resource/revisions");
    }
  }

  private static void validateOperation(final JsonElement operation, final boolean requireDeweyMetadata) {
    if (!operation.isJsonObject() || operation.getAsJsonObject().size() != 1) {
      throw new IllegalStateException("JSON diff sidecar operation must have exactly one kind");
    }

    final var entry = operation.getAsJsonObject().entrySet().iterator().next();
    if (!entry.getValue().isJsonObject()) {
      throw new IllegalStateException("JSON diff sidecar operation payload is not an object");
    }
    final JsonObject payload = entry.getValue().getAsJsonObject();
    if (requireDeweyMetadata) {
      validateDeweyMetadata(payload);
    }

    final boolean valid = switch (entry.getKey()) {
      case "insert" -> hasNodeKey(payload, "nodeKey") && hasNodeKey(payload, "insertPositionNodeKey")
          && hasAllowedString(payload, "insertPosition", "asFirstChild", "asRightSibling")
          && hasTypedValue(payload, "data", true);
      case "delete" -> hasNodeKey(payload, "nodeKey");
      case "update" -> hasNodeKey(payload, "nodeKey") && hasValidUpdatePayload(payload);
      case "replace" ->
        hasNodeKey(payload, "oldNodeKey") && hasNodeKey(payload, "newNodeKey") && hasTypedValue(payload, "data", true);
      default -> false;
    };
    if (!valid) {
      throw new IllegalStateException("JSON diff sidecar has an incomplete or unknown operation");
    }
  }

  private static boolean hasValidUpdatePayload(final JsonObject payload) {
    final boolean hasName = payload.has("name");
    final boolean hasTypedValue = payload.has("type") || payload.has("value");
    return (hasName || hasTypedValue) && (!hasName || hasString(payload, "name"))
        && (!hasTypedValue || hasTypedValue(payload, "value", false));
  }

  private static void validateDeweyMetadata(final JsonObject payload) {
    if (!hasString(payload, "deweyID") || !hasInt(payload, "depth")) {
      throw new IllegalStateException("JSON diff sidecar operation has no Dewey metadata");
    }
    try {
      final var deweyId = new SirixDeweyID(payload.get("deweyID").getAsString());
      final int depth = Integer.parseInt(payload.get("depth").getAsString());
      if (depth < 0 || depth != deweyId.getLevel()) {
        throw new IllegalStateException("JSON diff sidecar Dewey depth does not match its ID");
      }
    } catch (final IllegalArgumentException e) {
      throw new IllegalStateException("JSON diff sidecar Dewey metadata is malformed", e);
    }
  }

  private static boolean hasTypedValue(final JsonObject payload, final String valueName,
      final boolean allowUnmaterializedFragment) {
    if (!hasString(payload, "type")) {
      return false;
    }
    final String type = payload.get("type").getAsString();
    final JsonElement value = payload.get(valueName);
    return switch (type) {
      case "jsonFragment" -> allowUnmaterializedFragment
          && (value == null || value.isJsonPrimitive() && value.getAsJsonPrimitive().isString());
      case "boolean" -> value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isBoolean();
      case "string" -> value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString();
      case "number" -> value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber();
      case "null" -> value != null && value.isJsonNull();
      default -> false;
    };
  }

  private static boolean hasAllowedString(final JsonObject object, final String name, final String firstAllowed,
      final String secondAllowed) {
    if (!hasString(object, name)) {
      return false;
    }
    final String value = object.get(name).getAsString();
    return firstAllowed.equals(value) || secondAllowed.equals(value);
  }

  private static boolean hasStringValue(final JsonObject object, final String name, final String expected) {
    return hasString(object, name) && expected.equals(object.get(name).getAsString());
  }

  private static boolean hasIntValue(final JsonObject object, final String name, final int expected) {
    if (!hasInt(object, name)) {
      return false;
    }
    return Integer.parseInt(object.get(name).getAsString()) == expected;
  }

  private static boolean hasNodeKey(final JsonObject object, final String name) {
    final JsonElement value = object.get(name);
    if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
      return false;
    }
    try {
      return Long.parseLong(value.getAsString()) >= 0;
    } catch (final NumberFormatException e) {
      return false;
    }
  }

  private static boolean hasInt(final JsonObject object, final String name) {
    final JsonElement value = object.get(name);
    if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
      return false;
    }
    try {
      Integer.parseInt(value.getAsString());
      return true;
    } catch (final NumberFormatException e) {
      return false;
    }
  }

  private static boolean hasString(final JsonObject object, final String name) {
    final JsonElement value = object.get(name);
    return value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString();
  }

  private static void validateUnicodeScalarValues(final JsonElement element) {
    if (element.isJsonObject()) {
      for (final var entry : element.getAsJsonObject().entrySet()) {
        validateUnicodeScalarValue(entry.getKey());
        validateUnicodeScalarValues(entry.getValue());
      }
    } else if (element.isJsonArray()) {
      for (final JsonElement value : element.getAsJsonArray()) {
        validateUnicodeScalarValues(value);
      }
    } else if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
      validateUnicodeScalarValue(element.getAsString());
    }
  }

  private static void validateUnicodeScalarValue(final String value) {
    for (int index = 0; index < value.length(); index++) {
      final char current = value.charAt(index);
      if (Character.isHighSurrogate(current)) {
        if (++index >= value.length() || !Character.isLowSurrogate(value.charAt(index))) {
          throw new IllegalStateException("JSON diff sidecar contains an unpaired UTF-16 surrogate");
        }
      } else if (Character.isLowSurrogate(current)) {
        throw new IllegalStateException("JSON diff sidecar contains an unpaired UTF-16 surrogate");
      }
    }
  }
}
