package org.sirix.access.trx.node.xml;

import org.sirix.access.trx.node.InternalNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;

public interface InternalXmlNodeReadOnlyTrx extends InternalNodeReadOnlyTrx<ImmutableXmlNode>, XmlNodeReadOnlyTrx {

}
