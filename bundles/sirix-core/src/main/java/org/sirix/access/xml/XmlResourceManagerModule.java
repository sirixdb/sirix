package org.sirix.access.xml;

import dagger.Binds;
import dagger.Module;
import org.sirix.access.trx.node.xml.XmlResourceSessionImpl;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.dagger.ResourceManagerScope;

/**
 * The module for {@link XmlResourceManagerComponent}.
 *
 * @author Joao Sousa
 */
@Module
public interface XmlResourceManagerModule {

    @Binds
    @ResourceManagerScope
    XmlResourceSession resourceManager(XmlResourceSessionImpl resourceManager);
}
