package io.sirix.query.node;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Scope;
import io.brackit.query.jdm.Stream;
import org.jspecify.annotations.Nullable;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixException;

/**
 * Sirix scope.
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
    nodeKey = rtx.getNodeKey();
  }

  @Override
  public Stream<String> localPrefixes() {
    rtx.moveTo(nodeKey);
    return new Stream<>() {
      private int index;

      private final int mNamespaces = rtx.getNamespaceCount();

      @Override
      public String next() throws DocumentException {
        if (index < mNamespaces) {
          rtx.moveTo(nodeKey);
          rtx.moveToNamespace(index++);
          final int prefixKey = rtx.getPrefixKey();
          final String prefix = prefixKey == -1 ? "" : rtx.nameForKey(prefixKey);
          rtx.moveToParent();
          return prefix;
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
    final int prefixVocID = (prefix == null || prefix.isEmpty())
        ? -1
        : rtx.keyForName(prefix);
    rtx.moveTo(nodeKey);
    try {
      do {
        for (int i = 0, namespaces = rtx.getNamespaceCount(); i < namespaces; i++) {
          rtx.moveToNamespace(i);
          if (rtx.getPrefixKey() == prefixVocID) {
            return rtx.nameForKey(rtx.getURIKey());
          }
          rtx.moveToParent();
        }
      } while (rtx.moveToParent());
      if ("xml".equals(prefix)) {
        return "http://www.w3.org/XML/1998/namespace";
      }
      return prefixVocID == -1 ? "" : null;
    } finally {
      rtx.moveTo(nodeKey);
    }
  }

  @Override
  public void setDefaultNS(final String uri) {
    addPrefix("", uri);
  }
}
