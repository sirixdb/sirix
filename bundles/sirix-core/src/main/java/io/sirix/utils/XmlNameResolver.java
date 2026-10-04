package io.sirix.utils;

import io.brackit.query.atomic.QNm;
import io.sirix.api.StorageEngineReader;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNameNode;

import static java.util.Objects.requireNonNull;

/**
 * Resolves XML expanded names for NAME indexes from authoritative dictionary keys.
 *
 * <p>
 * Page-backed views may lack a cached name, and rebinding a write singleton can retain a cached
 * name from another node. Index delivery must resolve the currently addressed node's keys.
 */
public final class XmlNameResolver {
  private XmlNameResolver() {}

  public static QNm resolveExpandedName(final ImmutableNameNode node, final StorageEngineReader reader) {
    requireNonNull(node);
    requireNonNull(reader);
    final NodeKind kind = node.getKind();
    final int uriKey = node.getURIKey();
    final int localNameKey = node.getLocalNameKey();
    final String uri = uriKey == -1
        ? ""
        : reader.getName(uriKey, NodeKind.NAMESPACE);
    final String localName = localNameKey == -1
        ? ""
        : reader.getName(localNameKey, kind);
    // Prefix aliases have the same identity; avoid their dictionary lookup on every posting.
    return new QNm(uri, "", localName);
  }
}
