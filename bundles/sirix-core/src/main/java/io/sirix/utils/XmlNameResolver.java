package io.sirix.utils;

import io.brackit.query.atomic.QNm;
import io.sirix.api.StorageEngineReader;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNameNode;

import static java.util.Objects.requireNonNull;

/** Resolves XML names whose transient cache is absent on page-backed node views. */
public final class XmlNameResolver {
  private XmlNameResolver() {}

  /**
   * Returns the cached name or reconstructs it from the revision-local name dictionary. Decoded
   * records use {@link NodeKind#EMPTY_QNM} as an absent-cache sentinel.
   */
  public static QNm resolveName(final ImmutableNameNode node, final StorageEngineReader reader) {
    requireNonNull(node);
    requireNonNull(reader);
    final QNm cachedName = node.getName();
    if (cachedName != null && cachedName != NodeKind.EMPTY_QNM) {
      return cachedName;
    }
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
