package io.sirix.utils;

import io.brackit.query.atomic.QNm;
import io.sirix.api.StorageEngineReader;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNameNode;

import static java.util.Objects.requireNonNull;

/**
 * Resolves XML names from authoritative dictionary keys.
 *
 * <p>
 * Page-backed views may lack a cached name, and rebinding a write singleton can retain a cached
 * name from another node. Index delivery must resolve the currently addressed node's keys.
 */
public final class XmlNameResolver {
  private XmlNameResolver() {}

  public static QNm resolveName(final ImmutableNameNode node, final StorageEngineReader reader) {
    requireNonNull(node);
    requireNonNull(reader);
    final NodeKind kind = node.getKind();
    final int uriKey = node.getURIKey();
    final int prefixKey = node.getPrefixKey();
    final int localNameKey = node.getLocalNameKey();
    final String uri = reader.getName(uriKey, NodeKind.NAMESPACE);
    final String prefix = prefixKey == -1
        ? ""
        : reader.getName(prefixKey, kind);
    final String localName = localNameKey == -1
        ? ""
        : reader.getName(localNameKey, kind);
    return new QNm(uri, prefix, localName);
  }
}
