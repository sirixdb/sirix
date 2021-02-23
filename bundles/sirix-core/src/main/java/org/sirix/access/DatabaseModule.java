package org.sirix.access;

import dagger.Binds;
import dagger.Module;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.api.xml.XmlResourceManager;

/**
 * The DI module that bridges interfaces to implementations.
 *
 * @author Joao Sousa
 */
@Module
public interface DatabaseModule {

    @Binds
    LocalDatabaseFactory<JsonResourceManager> bindJsonDatabaseFactory(LocalJsonDatabaseFactory jsonFactory);

    @Binds
    LocalDatabaseFactory<XmlResourceManager> bindXmlDatabaseFactory(LocalXmlDatabaseFactory xmlFactory);
}
