package org.sirix.index.cas.xdm;

import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.index.cas.CASIndex;

public interface XdmCASIndex extends CASIndex<XdmCASIndexBuilder, XdmCASIndexListener, XdmNodeReadOnlyTrx> {
}
