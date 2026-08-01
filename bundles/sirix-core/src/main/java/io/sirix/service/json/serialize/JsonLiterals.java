package io.sirix.service.json.serialize;

import io.sirix.node.NodeKind;

/**
 * The fixed wire text shared by the JSON emitters ({@link JsonSerializer},
 * {@link JsonLimitedSerializer}): the pre-quoted metadata/envelope key literals, and the
 * pre-quoted {@link NodeKind} names emitted as the {@code "type"} value.
 *
 * <p>Single-sourced deliberately. The limited and unlimited emitters must produce identical
 * envelopes for the same node — a paginated REST read must not disagree with a full read on a
 * field name — and until this class each emitter carried its own private copy of these strings,
 * so a one-sided edit would have forked the wire format silently. Hoisting them is also the
 * per-node win the emitters rely on: building {@code "\"" + name + "\""} at every use was one
 * StringBuilder and one String per occurrence, several times per emitted node in metadata mode.
 */
final class JsonLiterals {

  static final String QUOTED_KEY = "\"key\"";
  static final String QUOTED_METADATA = "\"metadata\"";
  static final String QUOTED_NODE_KEY = "\"nodeKey\"";
  static final String QUOTED_HASH = "\"hash\"";
  static final String QUOTED_TYPE = "\"type\"";
  static final String QUOTED_DESCENDANT_COUNT = "\"descendantCount\"";
  static final String QUOTED_CHILD_COUNT = "\"childCount\"";
  static final String QUOTED_VALUE = "\"value\"";
  static final String QUOTED_SIRIX = "\"sirix\"";
  static final String QUOTED_REVISION = "\"revision\"";
  static final String QUOTED_REVISION_NUMBER = "\"revisionNumber\"";
  static final String QUOTED_REVISION_TIMESTAMP = "\"revisionTimestamp\"";

  /**
   * {@code "\"" + kind + "\""} per {@link NodeKind}, built once. The {@code "type"} value is
   * drawn from this small fixed set, and both emitters used to re-concatenate it per node.
   * Indexed by the kind's id (a byte, so 256 covers every possible value).
   */
  private static final String[] QUOTED_KIND_BY_ID = new String[256];

  static {
    for (final NodeKind kind : NodeKind.values()) {
      QUOTED_KIND_BY_ID[kind.getId() & 0xFF] = "\"" + kind + "\"";
    }
  }

  /** The pre-quoted name of {@code kind}, for the metadata {@code "type"} field. */
  static String quotedKind(final NodeKind kind) {
    return QUOTED_KIND_BY_ID[kind.getId() & 0xFF];
  }

  private JsonLiterals() {
  }
}
