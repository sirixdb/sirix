package io.sirix.access.xml;

import dagger.Subcomponent;
import io.sirix.access.GenericLocalDatabaseComponent;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.dagger.DatabaseScope;

/**
 * A Dagger subcomponent that manages {@link Database xml database sessions}.
 *
 * @author Joao Sousa
 */
@DatabaseScope
@Subcomponent(modules = {XmlLocalDatabaseModule.class})
public interface XmlLocalDatabaseComponent extends GenericLocalDatabaseComponent<XmlResourceSession,
        XmlResourceManagerComponent.Builder> {

    @Subcomponent.Builder
    interface Builder extends GenericLocalDatabaseComponent.Builder<Builder> {

        @Override
        XmlLocalDatabaseComponent build();
    }
}
