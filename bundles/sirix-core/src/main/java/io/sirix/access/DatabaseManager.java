package io.sirix.access;

import dagger.Component;
import io.sirix.access.json.JsonLocalDatabaseComponent;
import io.sirix.access.xml.XmlLocalDatabaseComponent;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;

import javax.inject.Singleton;

/**
 * The Dagger component that manages database dependencies. This class is
 * internal and managed by {@link Databases}.
 *
 * @author Joao Sousa
 */
@Component(modules = DatabaseModule.class)
@Singleton
public interface DatabaseManager {

	/**
	 * Creates a new Json database subcomponent.
	 *
	 * <p>
	 * This method is declared here in order to create a link between this component
	 * and {@link JsonLocalDatabaseComponent}, as parent component and subcomponent,
	 * respectively. Hence, it should not be called. Use
	 * {@link #jsonDatabaseFactory()} instead.
	 *
	 * @return A builder, used to create a new json database subcomponent.
	 */
	@SuppressWarnings("unused")
	JsonLocalDatabaseComponent.Builder jsonDatabaseBuilder();

	/**
	 * Creates a new Json database subcomponent.
	 *
	 * <p>
	 * This method is declared here in order to create a link between this component
	 * and {@link XmlLocalDatabaseComponent}, as parent component and subcomponent,
	 * respectively. Hence, it should not be called. Use
	 * {@link #xmlDatabaseFactory()} instead.
	 *
	 * @return A builder, used to create a new json database subcomponent.
	 */
	@SuppressWarnings("unused")
	XmlLocalDatabaseComponent.Builder xmlDatabaseBuilder();

	LocalDatabaseFactory<JsonResourceSession> jsonDatabaseFactory();

	LocalDatabaseFactory<XmlResourceSession> xmlDatabaseFactory();

	PathBasedPool<Database<?>> sessions();

}
