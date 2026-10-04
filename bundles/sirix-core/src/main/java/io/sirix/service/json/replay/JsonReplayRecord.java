package io.sirix.service.json.replay;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Detached logical document record. Keys describe the final graph, never an insertion program.
 * Path keys belong to the manifest's source namespace and must be resolved by the importer.
 */
public record JsonReplayRecord(long key, NodeKind kind, long parent, long left, long right,
    long firstChild, long lastChild, long childCount, long descendantCount, long pathKey, int nameKey,
    int previousRevision, int lastModifiedRevision, long hash, @Nullable SirixDeweyID deweyID,
    @Nullable String name, @Nullable String stringValue, @Nullable Number numberValue, boolean booleanValue) {

  public JsonReplayRecord {
    Objects.requireNonNull(kind);
    if (key < 0 || parent < -1 || left < -1 || right < -1 || firstChild < -1 || lastChild < -1
        || childCount < 0 || descendantCount < 0 || pathKey < -1) {
      throw new IllegalArgumentException("Invalid replay record keys or counts");
    }
    if ((key == 0) != (kind == NodeKind.JSON_DOCUMENT)) {
      throw new IllegalArgumentException("Only the document may have identity zero");
    }
    if (kind.playsObjectKeyRole() != (name != null)) {
      throw new IllegalArgumentException("Object field name does not match replay record kind");
    }
    switch (kind) {
      case STRING_VALUE, OBJECT_NAMED_STRING -> Objects.requireNonNull(stringValue);
      case NUMBER_VALUE, OBJECT_NAMED_NUMBER -> Objects.requireNonNull(numberValue);
      case JSON_DOCUMENT, ARRAY, OBJECT, BOOLEAN_VALUE, NULL_VALUE, OBJECT_NAMED_ARRAY,
          OBJECT_NAMED_OBJECT, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NULL -> { }
      default -> throw new IllegalArgumentException("Not a JSON document record: " + kind);
    }
  }

  /** Capture before the cursor or any page-backed singleton is reused. */
  public static JsonReplayRecord capture(final JsonNodeReadOnlyTrx reader) {
    Objects.requireNonNull(reader);
    if (reader.isFusedSyntheticChild()) {
      throw new IllegalArgumentException("A synthetic value has no independent persistent identity");
    }
    final NodeKind kind = reader.getKind();
    final String name = kind.playsObjectKeyRole() ? reader.getName().getLocalName() : null;
    final String string = kind == NodeKind.STRING_VALUE || kind == NodeKind.OBJECT_NAMED_STRING
        ? reader.getValue() : null;
    final Number number = kind == NodeKind.NUMBER_VALUE || kind == NodeKind.OBJECT_NAMED_NUMBER
        ? reader.getNumberValue() : null;
    final boolean bool = (kind == NodeKind.BOOLEAN_VALUE || kind == NodeKind.OBJECT_NAMED_BOOLEAN)
        && reader.getBooleanValue();
    return new JsonReplayRecord(reader.getNodeKey(), kind, reader.getParentKey(), reader.getLeftSiblingKey(),
        reader.getRightSiblingKey(), reader.getFirstChildKey(), reader.getLastChildKey(), reader.getChildCount(),
        reader.getDescendantCount(), reader.getPathNodeKey(), name == null ? -1 : reader.getNameKey(), reader.getPreviousRevisionNumber(),
        reader.getNode().getLastModifiedRevisionNumber(), reader.getHash(), reader.getDeweyID(), name,
        string, number, bool);
  }
}
