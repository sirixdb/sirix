package io.sirix.query.node;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Scope;
import io.brackit.query.jdm.Stream;
import org.jspecify.annotations.Nullable;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixException;

import javax.xml.XMLConstants;

/**
 * Namespace scope anchored to a stored XML element. Nodes share the transaction cursor, so scope
 * operations must reposition it to the owning element even when other node accesses have moved it.
 * Prefix resolution searches local declarations before ancestors and restores the owning element.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixScope implements Scope {

  /** Sirix {@link XmlNodeReadOnlyTrx}. */
  private final XmlNodeReadOnlyTrx rtx;

  /** Owning element, independent of the shared transaction cursor. */
  private final long nodeKey;

  /**
   * Constructor.
   *
   * @param node database node
   */
  public SirixScope(final XmlDBNode node) {
    // Assertion instead of requireNonNull(...) (part of internal API).
    assert node != null;
    rtx = node.getTrx();
    nodeKey = node.getNodeKey();
  }

  @Override
  public Stream<String> localPrefixes() {
    final long currentNodeKey = rtx.getNodeKey();
    final int namespaces;
    try {
      rtx.moveTo(nodeKey);
      namespaces = rtx.getNamespaceCount();
    } finally {
      rtx.moveTo(currentNodeKey);
    }
    return new Stream<>() {
      private int index;

      @Override
      public String next() throws DocumentException {
        if (index < namespaces) {
          final long currentNodeKey = rtx.getNodeKey();
          try {
            rtx.moveTo(nodeKey);
            rtx.moveToNamespace(index++);
            final int prefixKey = rtx.getPrefixKey();
            return prefixKey == -1
                ? ""
                : rtx.nameForKey(prefixKey);
          } finally {
            rtx.moveTo(currentNodeKey);
          }
        }
        return null;
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public String defaultNS() {
    return resolvePrefix("");
  }

  @Override
  public void addPrefix(final String prefix, final String uri) {
    if (rtx instanceof final XmlNodeTrx wtx) {
      wtx.moveTo(nodeKey);
      try {
        wtx.insertNamespace(new QNm(uri, prefix, ""));
      } catch (final SirixException e) {
        throw new DocumentException(e);
      } finally {
        wtx.moveTo(nodeKey);
      }
    }
  }

  @Override
  public String resolvePrefix(final @Nullable String prefix) {
    if ("xml".equals(prefix)) {
      return XMLConstants.XML_NS_URI;
    }
    final String resolvedPrefix = prefix == null
        ? ""
        : prefix;
    final long currentNodeKey = rtx.getNodeKey();
    try {
      rtx.moveTo(nodeKey);
      while (rtx.isElement()) {
        for (int i = 0, namespaces = rtx.getNamespaceCount(); i < namespaces; i++) {
          rtx.moveToNamespace(i);
          final int prefixKey = rtx.getPrefixKey();
          if (prefixKey == -1
              ? resolvedPrefix.isEmpty()
              : resolvedPrefix.equals(rtx.nameForKey(prefixKey))) {
            return rtx.getValue();
          }
          rtx.moveToParent();
        }
        if (!rtx.moveToParent()) {
          break;
        }
      }
    } finally {
      rtx.moveTo(currentNodeKey);
    }
    return resolvedPrefix.isEmpty()
        ? ""
        : null;
  }

  @Override
  public void setDefaultNS(final String uri) {
    addPrefix("", uri);
  }
}
