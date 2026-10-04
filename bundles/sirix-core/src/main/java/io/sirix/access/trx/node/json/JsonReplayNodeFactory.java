package io.sirix.access.trx.node.json;

import io.sirix.node.interfaces.StructNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.StringNode;
import io.sirix.service.json.replay.JsonReplayManifest;
import io.sirix.service.json.replay.JsonReplayRecord;
import io.sirix.settings.Constants;
import net.openhft.hashing.LongHashFunction;

/** Explicit identities are confined to the import epoch; ordinary factories remain allocation-only. */
final class JsonReplayNodeFactory {
  private JsonReplayNodeFactory() {
  }

  /** Build an unlinked payload. No final parent, sibling or child needs to exist yet. */
  static StructNode stage(final JsonReplayRecord record, final JsonReplayManifest manifest,
      final LongHashFunction hashFunction) {
    final long key = record.key();
    final int previous = manifest.mapPreviousRevision(record.previousRevision());
    final int modified = manifest.mapLastModifiedRevision(record.lastModifiedRevision());
    final var dewey = record.deweyID();
    final int nameKey = record.nameKey();
    final long pathKey = record.pathKey();
    return switch (record.kind()) {
      case JSON_DOCUMENT -> new JsonDocumentRootNode(key, -1, -1, 0, 0, hashFunction, dewey);
      case ARRAY -> new ArrayNode(key, -1, pathKey, previous, modified, -1, -1, -1, -1,
          0, 0, record.hash(), hashFunction, dewey);
      case OBJECT -> new ObjectNode(key, -1, previous, modified, -1, -1, -1, -1,
          0, 0, record.hash(), hashFunction, dewey);
      case BOOLEAN_VALUE -> new BooleanNode(key, -1, previous, modified, -1, -1, record.hash(),
          record.booleanValue(), hashFunction, dewey);
      case NULL_VALUE -> new NullNode(key, -1, previous, modified, -1, -1, record.hash(), hashFunction, dewey);
      case NUMBER_VALUE -> new NumberNode(key, -1, previous, modified, -1, -1, record.hash(),
          record.numberValue(), hashFunction, dewey);
      case STRING_VALUE -> new StringNode(key, -1, previous, modified, -1, -1, record.hash(),
          record.stringValue().getBytes(Constants.DEFAULT_ENCODING), hashFunction, dewey);
      case OBJECT_NAMED_ARRAY -> new ObjectNamedArrayNode(key, -1, -1, -1, -1, -1, nameKey, pathKey,
          previous, modified, record.hash(), 0, 0, hashFunction, dewey);
      case OBJECT_NAMED_OBJECT -> new ObjectNamedObjectNode(key, -1, -1, -1, -1, -1, nameKey, pathKey,
          previous, modified, record.hash(), 0, 0, hashFunction, dewey);
      case OBJECT_NAMED_NUMBER -> new ObjectNamedNumberNode(key, -1, -1, -1, nameKey, pathKey,
          previous, modified, record.hash(), record.numberValue(), hashFunction, dewey);
      case OBJECT_NAMED_STRING -> new ObjectNamedStringNode(key, -1, -1, -1, nameKey, pathKey,
          previous, modified, record.hash(), record.stringValue().getBytes(Constants.DEFAULT_ENCODING), hashFunction,
          dewey);
      case OBJECT_NAMED_BOOLEAN -> new ObjectNamedBooleanNode(key, -1, -1, -1, nameKey, pathKey,
          previous, modified, record.hash(), record.booleanValue(), hashFunction, dewey);
      case OBJECT_NAMED_NULL -> new ObjectNamedNullNode(key, -1, -1, -1, nameKey, pathKey,
          previous, modified, record.hash(), hashFunction, dewey);
      default -> throw new IllegalArgumentException("Unsupported replay kind " + record.kind());
    };
  }

  static void link(final StructNode node, final JsonReplayRecord record) {
    // Scalar implementations reject non-empty child links/counts, even for a setter of zero.
    if (record.key() != 0) {
      node.setParentKey(record.parent());
      node.setLeftSiblingKey(record.left());
      node.setRightSiblingKey(record.right());
    }
    switch (record.kind()) {
      case JSON_DOCUMENT, ARRAY, OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_OBJECT -> {
        node.setFirstChildKey(record.firstChild());
        node.setLastChildKey(record.lastChild());
        node.setChildCount(record.childCount());
        node.setDescendantCount(record.descendantCount());
      }
      default -> { }
    }
    node.setHash(record.hash());
  }
}
