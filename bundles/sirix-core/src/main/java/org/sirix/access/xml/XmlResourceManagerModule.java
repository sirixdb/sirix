package org.sirix.access.xml;

import dagger.Binds;
import dagger.Module;
import org.sirix.access.trx.node.xml.XmlResourceManagerImpl;
import org.sirix.api.xml.XmlResourceManager;
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
    XmlResourceManager resourceManager(XmlResourceManagerImpl resourceManager);
}
