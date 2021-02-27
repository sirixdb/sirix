package org.sirix.access.xml;

import dagger.Subcomponent;
import org.sirix.access.GenericResourceManagerComponent;
import org.sirix.access.ResourceManagerModule;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.dagger.ResourceManagerScope;

/**
 * A {@link Subcomponent dagger subcomponent} that manages the lifecycle of a {@link XmlResourceManager}.
 *
 * @author Joao Sousa
 */
@ResourceManagerScope
@Subcomponent(modules = {XmlResourceManagerModule.class, ResourceManagerModule.class})
public interface XmlResourceManagerComponent extends GenericResourceManagerComponent<XmlResourceManager> {

    @Subcomponent.Builder
    interface Builder extends GenericResourceManagerComponent.Builder<Builder, XmlResourceManager,
            XmlResourceManagerComponent> {

    }
}
