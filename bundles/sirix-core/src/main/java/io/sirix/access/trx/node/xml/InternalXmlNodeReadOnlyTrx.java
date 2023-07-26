package io.sirix.access.trx.node.xml;

import io.sirix.access.trx.node.InternalNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;

public interface InternalXmlNodeReadOnlyTrx extends InternalNodeReadOnlyTrx<ImmutableXmlNode>, XmlNodeReadOnlyTrx {

}
