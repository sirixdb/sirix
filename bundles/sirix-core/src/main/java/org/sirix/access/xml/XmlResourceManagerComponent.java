package org.sirix.access.xml;

import dagger.Subcomponent;
import org.sirix.access.GenericResourceSessionComponent;
import org.sirix.access.ResourceSessionModule;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.dagger.ResourceManagerScope;

/**
 * A {@link Subcomponent dagger subcomponent} that manages the lifecycle of a {@link XmlResourceSession}.
 *
 * @author Joao Sousa
 */
@ResourceManagerScope
@Subcomponent(modules = {XmlResourceManagerModule.class, ResourceSessionModule.class})
public interface XmlResourceManagerComponent extends GenericResourceSessionComponent<XmlResourceSession> {

    @Subcomponent.Builder
    interface Builder extends GenericResourceSessionComponent.Builder<Builder, XmlResourceSession,
            XmlResourceManagerComponent> {

    }
}
