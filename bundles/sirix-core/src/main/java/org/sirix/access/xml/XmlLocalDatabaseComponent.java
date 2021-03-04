package org.sirix.access.xml;

import dagger.Subcomponent;
import org.sirix.access.GenericLocalDatabaseComponent;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.dagger.DatabaseScope;

/**
 * A Dagger subcomponent that manages {@link Database xml database sessions}.
 *
 * @author Joao Sousa
 */
@DatabaseScope
@Subcomponent(modules = {XmlLocalDatabaseModule.class})
public interface XmlLocalDatabaseComponent extends GenericLocalDatabaseComponent<XmlResourceManager> {

    @Subcomponent.Builder
    interface Builder extends GenericLocalDatabaseComponent.Builder<Builder> {

        @Override
        XmlLocalDatabaseComponent build();
    }
}
