package org.sirix.utils;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

public class Util {
  /**
   * Building a {@link QName} from a URI and a name. The name can have a prefix denoted
   * by a ":" delimiter.
   * 
   * @param pUri
   *          the namespaceuri
   * @param pName
   *          the name including a possible prefix
   * @return {@link QName} instance
   */
  public static final QName buildQName(@Nonnull final String pUri,
    @Nonnull final String pName) {
    QName qname;
    if (pName.contains(":")) {
      qname = new QName(pUri, pName.split(":")[1], pName.split(":")[0]);
    } else {
      qname = new QName(pUri, pName);
    }
    return qname;
  }
}
