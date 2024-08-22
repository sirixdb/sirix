package io.sirix.access.xml;

import dagger.Subcomponent;
import io.sirix.access.GenericResourceSessionComponent;
import io.sirix.access.ResourceSessionModule;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.dagger.ResourceSessionScope;

/**
 * A {@link Subcomponent dagger subcomponent} that manages the lifecycle of a
 * {@link XmlResourceSession}.
 *
 * @author Joao Sousa
 */
@ResourceSessionScope
@Subcomponent(modules = {XmlResourceManagerModule.class, ResourceSessionModule.class})
public interface XmlResourceManagerComponent extends GenericResourceSessionComponent<XmlResourceSession> {

	@Subcomponent.Builder
	interface Builder
			extends
				GenericResourceSessionComponent.Builder<Builder, XmlResourceSession, XmlResourceManagerComponent> {

	}
}
