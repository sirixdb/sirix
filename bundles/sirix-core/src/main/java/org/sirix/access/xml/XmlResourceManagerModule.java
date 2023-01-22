package org.sirix.access.xml;

import dagger.Binds;
import dagger.Module;
import org.sirix.access.trx.node.xml.XmlResourceSessionImpl;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.dagger.ResourceSessionScope;

/**
 * The module for {@link XmlResourceManagerComponent}.
 *
 * @author Joao Sousa
 */
@Module
public interface XmlResourceManagerModule {

    @Binds
    @ResourceSessionScope
    XmlResourceSession resourceSession(XmlResourceSessionImpl resourceSession);
}
