package io.sirix.mcp;

import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonResourceSession;
import com.google.gson.JsonElement;
import io.sirix.diff.DiffDepth;
import io.sirix.diff.DiffFactory;
import io.sirix.diff.DiffObserver;
import io.sirix.diff.DiffTuple;
import io.sirix.diff.JsonDiffSerializer;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.mcp.LimitedStringWriter.LimitExceededException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Retains one page of tuples and bounds serialized payloads before exposing complete operations.
 */
final class BoundedDiff implements DiffObserver {
  private static final JsonMapper MAPPER = JsonMapper.builder()
                                                     .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                                                     .disable(SerializationFeature.WRAP_EXCEPTIONS)
                                                     .build();
  private static final TypeReference<List<?>> OPERATIONS_TYPE = new TypeReference<>() {};
  private final ArrayList<DiffTuple> tuples;
  private final int offset;
  private final int limit;
  private int visited;
  private boolean truncated;

  private BoundedDiff(final int offset, final int limit) {
    this.offset = offset;
    this.limit = limit;
    tuples = new ArrayList<>(Math.min(limit, 128));
  }

  static Map<String, Object> generate(final String database, final JsonResourceSession session, final int from,
      final int to, final int offset, final McpServerConfig config) {
    final var observer = new BoundedDiff(offset, config.maxResultSize());
    final var optimized = session.getResourceConfig().hashType == HashType.NONE
        ? DiffFactory.DiffOptimized.NO
        : DiffFactory.DiffOptimized.HASHED;
    DiffFactory.invokeJsonDiff(
        new DiffFactory.Builder<>(session, to, from, optimized, Set.of(observer)).skipSubtrees(true)
                                                                                 .newStartKey(0)
                                                                                 .oldStartKey(0));
    boolean truncated = observer.truncated;
    final var changes = new ArrayList<Object>(observer.tuples.size());
    int consumed = 0;
    int characters = 0;
    boolean oversized = false;
    try (final var serializer = new JsonDiffSerializer(database, session, from, to, observer.tuples).operations();
        final var writer = new LimitedStringWriter(config.maxStringValueLength())) {
      for (final DiffTuple tuple : observer.tuples) {
        final var serialized = serializer.serialize(tuple);
        if (remainingBudget(serialized, config.maxStringValueLength() - characters) < 0) {
          oversized = true;
          truncated = true;
          break;
        }
        final List<?> operations = MAPPER.readValue(serialized.toString(), OPERATIONS_TYPE);
        if (!hydrateFragments(operations, session, to, tuple.getNewNodeKey(),
            config.maxStringValueLength() - characters, writer)) {
          oversized = true;
          truncated = true;
          break;
        }
        writer.reset(config.maxStringValueLength() - characters);
        try {
          MAPPER.writeValue(writer, operations);
        } catch (final LimitExceededException exception) {
          oversized = true;
          truncated = true;
          break;
        }
        characters += writer.length();
        changes.addAll(operations);
        consumed++;
      }
    }
    final var result = new LinkedHashMap<String, Object>();
    result.put("database", database);
    result.put("resource", session.getResourceConfig().getName());
    result.put("old-revision", from);
    result.put("new-revision", to);
    result.put("diffs", changes);
    result.put("truncated", truncated);
    if (truncated) {
      result.put("continuation", Map.of("offset", offset + consumed, "hint", oversized && consumed == 0
          ? "Change exceeds maxStringValueLength; query the changed node with a smaller projection"
          : "Repeat the same diff revision pair with this offset"));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static boolean hydrateFragments(final List<?> operations, final JsonResourceSession session,
      final int revision, final long nodeKey, final int budget, final LimitedStringWriter writer) {
    for (final Object operation : operations) {
      final Map<String, Object> wrapper = (Map<String, Object>) operation;
      final Object payload = wrapper.containsKey("insert")
          ? wrapper.get("insert")
          : wrapper.get("replace");
      if (!(payload instanceof Map<?, ?> fragment) || !"jsonFragment".equals(fragment.get("type")))
        continue;
      writer.reset(budget);
      try {
        JsonSerializer.newBuilder(session, writer, revision).startNodeKey(nodeKey).build().call();
        ((Map<String, Object>) fragment).put("data", writer.toString());
      } catch (final LimitExceededException exception) {
        return false;
      }
    }
    return true;
  }

  private static int remainingBudget(final JsonElement value, int remaining) {
    if (value.isJsonNull())
      return remaining - 4;
    if (value.isJsonPrimitive())
      return remaining - value.getAsString().length();
    if (value.isJsonObject()) {
      for (final var field : value.getAsJsonObject().entrySet()) {
        remaining -= field.getKey().length();
        if (remaining < 0)
          return remaining;
        remaining = remainingBudget(field.getValue(), remaining);
        if (remaining < 0)
          return remaining;
      }
    } else {
      for (final var item : value.getAsJsonArray()) {
        remaining = remainingBudget(item, remaining);
        if (remaining < 0)
          return remaining;
      }
    }
    return remaining;
  }

  @Override
  public void diffListener(final DiffFactory.DiffType type, final long newKey, final long oldKey,
      final DiffDepth depth) {
    if (type == DiffFactory.DiffType.SAME || type == DiffFactory.DiffType.SAMEHASH
        || type == DiffFactory.DiffType.REPLACEDOLD)
      return;
    if (visited++ < offset)
      return;
    if (tuples.size() == limit) {
      truncated = true;
      return;
    }
    tuples.add(new DiffTuple(type, newKey, oldKey, depth));
  }

  @Override
  public void diffDone() {}

}
