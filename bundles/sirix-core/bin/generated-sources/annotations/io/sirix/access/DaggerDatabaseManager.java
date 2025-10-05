package io.sirix.access;

import dagger.internal.DoubleCheck;
import dagger.internal.InstanceFactory;
import dagger.internal.Preconditions;
import io.sirix.access.json.JsonLocalDatabaseComponent;
import io.sirix.access.json.JsonLocalDatabaseModule_JsonDatabaseFactory;
import io.sirix.access.json.JsonLocalDatabaseModule_JsonResourceManagerFactory;
import io.sirix.access.json.JsonLocalDatabaseModule_ResourceManagerFactoryFactory;
import io.sirix.access.json.JsonResourceSessionComponent;
import io.sirix.access.json.LocalJsonDatabaseFactory;
import io.sirix.access.json.LocalJsonDatabaseFactory_Factory;
import io.sirix.access.trx.TransactionManagerImpl_Factory;
import io.sirix.access.trx.node.json.JsonResourceSessionImpl;
import io.sirix.access.trx.node.json.JsonResourceSessionImpl_Factory;
import io.sirix.access.trx.node.xml.XmlResourceSessionImpl;
import io.sirix.access.trx.node.xml.XmlResourceSessionImpl_Factory;
import io.sirix.access.trx.page.PageTrxFactory;
import io.sirix.access.trx.page.PageTrxFactory_Factory;
import io.sirix.access.xml.LocalXmlDatabaseFactory;
import io.sirix.access.xml.LocalXmlDatabaseFactory_Factory;
import io.sirix.access.xml.XmlLocalDatabaseComponent;
import io.sirix.access.xml.XmlLocalDatabaseModule_ResourceManagerFactoryFactory;
import io.sirix.access.xml.XmlLocalDatabaseModule_XmlDatabaseFactory;
import io.sirix.access.xml.XmlLocalDatabaseModule_XmlResourceManagerFactory;
import io.sirix.access.xml.XmlResourceManagerComponent;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.TransactionManager;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.io.IOStorage;
import io.sirix.page.UberPage;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class DaggerDatabaseManager implements DatabaseManager {
  private Provider<JsonLocalDatabaseComponent.Builder> jsonLocalDatabaseComponentBuilderProvider;

  private Provider<LocalJsonDatabaseFactory> localJsonDatabaseFactoryProvider;

  private Provider<XmlLocalDatabaseComponent.Builder> xmlLocalDatabaseComponentBuilderProvider;

  private Provider<LocalXmlDatabaseFactory> localXmlDatabaseFactoryProvider;

  private Provider<PathBasedPool<Database<?>>> databaseSessionsProvider;

  private Provider<PathBasedPool<ResourceSession<?, ?>>> resourceManagersProvider;

  private Provider<WriteLocksRegistry> writeLocksRegistryProvider;

  private DaggerDatabaseManager() {

    initialize();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static DatabaseManager create() {
    return new Builder().build();
  }

  @SuppressWarnings("unchecked")
  private void initialize() {
    this.jsonLocalDatabaseComponentBuilderProvider = new Provider<JsonLocalDatabaseComponent.Builder>() {
      @Override
      public JsonLocalDatabaseComponent.Builder get() {
        return new JsonLocalDatabaseComponentBuilder();
      }
    };
    this.localJsonDatabaseFactoryProvider = DoubleCheck.provider(LocalJsonDatabaseFactory_Factory.create(jsonLocalDatabaseComponentBuilderProvider));
    this.xmlLocalDatabaseComponentBuilderProvider = new Provider<XmlLocalDatabaseComponent.Builder>() {
      @Override
      public XmlLocalDatabaseComponent.Builder get() {
        return new XmlLocalDatabaseComponentBuilder();
      }
    };
    this.localXmlDatabaseFactoryProvider = DoubleCheck.provider(LocalXmlDatabaseFactory_Factory.create(xmlLocalDatabaseComponentBuilderProvider));
    this.databaseSessionsProvider = DoubleCheck.provider(DatabaseModule_DatabaseSessionsFactory.create());
    this.resourceManagersProvider = DoubleCheck.provider(DatabaseModule_ResourceManagersFactory.create());
    this.writeLocksRegistryProvider = DoubleCheck.provider(WriteLocksRegistry_Factory.create());
  }

  @Override
  public JsonLocalDatabaseComponent.Builder jsonDatabaseBuilder() {
    return new JsonLocalDatabaseComponentBuilder();
  }

  @Override
  public XmlLocalDatabaseComponent.Builder xmlDatabaseBuilder() {
    return new XmlLocalDatabaseComponentBuilder();
  }

  @Override
  public LocalDatabaseFactory<JsonResourceSession> jsonDatabaseFactory() {
    return localJsonDatabaseFactoryProvider.get();
  }

  @Override
  public LocalDatabaseFactory<XmlResourceSession> xmlDatabaseFactory() {
    return localXmlDatabaseFactoryProvider.get();
  }

  @Override
  public PathBasedPool<Database<?>> sessions() {
    return databaseSessionsProvider.get();
  }

  public static final class Builder {
    private Builder() {
    }

    public DatabaseManager build() {
      return new DaggerDatabaseManager();
    }
  }

  private final class JsonLocalDatabaseComponentBuilder implements JsonLocalDatabaseComponent.Builder {
    private DatabaseConfiguration databaseConfiguration;

    private User user;

    @Override
    public JsonLocalDatabaseComponentBuilder databaseConfiguration(
        DatabaseConfiguration configuration) {
      this.databaseConfiguration = Preconditions.checkNotNull(configuration);
      return this;
    }

    @Override
    public JsonLocalDatabaseComponentBuilder user(User user) {
      this.user = Preconditions.checkNotNull(user);
      return this;
    }

    @Override
    public JsonLocalDatabaseComponent build() {
      Preconditions.checkBuilderRequirement(databaseConfiguration, DatabaseConfiguration.class);
      Preconditions.checkBuilderRequirement(user, User.class);
      return new JsonLocalDatabaseComponentImpl(databaseConfiguration, user);
    }
  }

  private final class JsonLocalDatabaseComponentImpl implements JsonLocalDatabaseComponent {
    private Provider<TransactionManager> transactionManagerProvider;

    private Provider<DatabaseConfiguration> databaseConfigurationProvider;

    private Provider<JsonResourceSessionComponent.Builder> resourceManagerBuilderProvider;

    private Provider<ResourceSessionFactory<JsonResourceSession>> resourceManagerFactoryProvider;

    private Provider<ResourceStore<JsonResourceSession>> jsonResourceManagerProvider;

    private Provider<Database<JsonResourceSession>> jsonDatabaseProvider;

    private Provider<User> userProvider;

    private Provider<String> databaseNameProvider;

    private Provider<DatabaseType> databaseTypeProvider;

    private JsonLocalDatabaseComponentImpl(DatabaseConfiguration databaseConfigurationParam,
        User userParam) {

      initialize(databaseConfigurationParam, userParam);
    }

    @SuppressWarnings("unchecked")
    private void initialize(final DatabaseConfiguration databaseConfigurationParam,
        final User userParam) {
      this.transactionManagerProvider = DoubleCheck.provider((Provider) TransactionManagerImpl_Factory.create());
      this.databaseConfigurationProvider = InstanceFactory.create(databaseConfigurationParam);
      this.resourceManagerBuilderProvider = new Provider<JsonResourceSessionComponent.Builder>() {
        @Override
        public JsonResourceSessionComponent.Builder get() {
          return new JsonResourceSessionComponentBuilder();
        }
      };
      this.resourceManagerFactoryProvider = DoubleCheck.provider(JsonLocalDatabaseModule_ResourceManagerFactoryFactory.create(resourceManagerBuilderProvider));
      this.jsonResourceManagerProvider = DoubleCheck.provider(JsonLocalDatabaseModule_JsonResourceManagerFactory.create(DaggerDatabaseManager.this.resourceManagersProvider, resourceManagerFactoryProvider));
      this.jsonDatabaseProvider = DoubleCheck.provider(JsonLocalDatabaseModule_JsonDatabaseFactory.create(transactionManagerProvider, databaseConfigurationProvider, DaggerDatabaseManager.this.databaseSessionsProvider, jsonResourceManagerProvider, DaggerDatabaseManager.this.writeLocksRegistryProvider, DaggerDatabaseManager.this.resourceManagersProvider));
      this.userProvider = InstanceFactory.create(userParam);
      this.databaseNameProvider = DoubleCheck.provider(LocalDatabaseModule_DatabaseNameFactory.create(databaseConfigurationProvider));
      this.databaseTypeProvider = DoubleCheck.provider(LocalDatabaseModule_DatabaseTypeFactory.create(databaseConfigurationProvider));
    }

    @Override
    public Database<JsonResourceSession> database() {
      return jsonDatabaseProvider.get();
    }

    @Override
    public JsonResourceSessionComponent.Builder resourceManagerBuilder() {
      return new JsonResourceSessionComponentBuilder();
    }

    private final class JsonResourceSessionComponentBuilder implements JsonResourceSessionComponent.Builder {
      private ResourceConfiguration resourceConfig;

      private BufferManager bufferManager;

      private Path resourceFile;

      @Override
      public JsonResourceSessionComponentBuilder resourceConfig(
          ResourceConfiguration resourceConfiguration) {
        this.resourceConfig = Preconditions.checkNotNull(resourceConfiguration);
        return this;
      }

      @Override
      public JsonResourceSessionComponentBuilder bufferManager(BufferManager bufferManager) {
        this.bufferManager = Preconditions.checkNotNull(bufferManager);
        return this;
      }

      @Override
      public JsonResourceSessionComponentBuilder resourceFile(Path resourceFile) {
        this.resourceFile = Preconditions.checkNotNull(resourceFile);
        return this;
      }

      @Override
      public JsonResourceSessionComponent build() {
        Preconditions.checkBuilderRequirement(resourceConfig, ResourceConfiguration.class);
        Preconditions.checkBuilderRequirement(bufferManager, BufferManager.class);
        Preconditions.checkBuilderRequirement(resourceFile, Path.class);
        return new JsonResourceSessionComponentImpl(resourceConfig, bufferManager, resourceFile);
      }
    }

    private final class JsonResourceSessionComponentImpl implements JsonResourceSessionComponent {
      private Provider<ResourceConfiguration> resourceConfigProvider;

      private Provider<BufferManager> bufferManagerProvider;

      private Provider<IOStorage> ioStorageProvider;

      private Provider<UberPage> rootPageProvider;

      private Provider<Semaphore> writeLockProvider;

      private Provider<PageTrxFactory> pageTrxFactoryProvider;

      private Provider<JsonResourceSessionImpl> jsonResourceSessionImplProvider;

      private Provider<JsonResourceSession> resourceSessionProvider;

      private JsonResourceSessionComponentImpl(ResourceConfiguration resourceConfigParam,
          BufferManager bufferManagerParam, Path resourceFile) {

        initialize(resourceConfigParam, bufferManagerParam, resourceFile);
      }

      @SuppressWarnings("unchecked")
      private void initialize(final ResourceConfiguration resourceConfigParam,
          final BufferManager bufferManagerParam, final Path resourceFile) {
        this.resourceConfigProvider = InstanceFactory.create(resourceConfigParam);
        this.bufferManagerProvider = InstanceFactory.create(bufferManagerParam);
        this.ioStorageProvider = DoubleCheck.provider(ResourceSessionModule_IoStorageFactory.create(resourceConfigProvider));
        this.rootPageProvider = DoubleCheck.provider(ResourceSessionModule_RootPageFactory.create(ioStorageProvider));
        this.writeLockProvider = DoubleCheck.provider(ResourceSessionModule_WriteLockFactory.create(DaggerDatabaseManager.this.writeLocksRegistryProvider, resourceConfigProvider));
        this.pageTrxFactoryProvider = PageTrxFactory_Factory.create(JsonLocalDatabaseComponentImpl.this.databaseTypeProvider);
        this.jsonResourceSessionImplProvider = JsonResourceSessionImpl_Factory.create(JsonLocalDatabaseComponentImpl.this.jsonResourceManagerProvider, resourceConfigProvider, bufferManagerProvider, ioStorageProvider, rootPageProvider, writeLockProvider, JsonLocalDatabaseComponentImpl.this.userProvider, JsonLocalDatabaseComponentImpl.this.databaseNameProvider, pageTrxFactoryProvider);
        this.resourceSessionProvider = DoubleCheck.provider((Provider) jsonResourceSessionImplProvider);
      }

      @Override
      public JsonResourceSession resourceManager() {
        return resourceSessionProvider.get();
      }
    }
  }

  private final class XmlLocalDatabaseComponentBuilder implements XmlLocalDatabaseComponent.Builder {
    private DatabaseConfiguration databaseConfiguration;

    private User user;

    @Override
    public XmlLocalDatabaseComponentBuilder databaseConfiguration(
        DatabaseConfiguration configuration) {
      this.databaseConfiguration = Preconditions.checkNotNull(configuration);
      return this;
    }

    @Override
    public XmlLocalDatabaseComponentBuilder user(User user) {
      this.user = Preconditions.checkNotNull(user);
      return this;
    }

    @Override
    public XmlLocalDatabaseComponent build() {
      Preconditions.checkBuilderRequirement(databaseConfiguration, DatabaseConfiguration.class);
      Preconditions.checkBuilderRequirement(user, User.class);
      return new XmlLocalDatabaseComponentImpl(databaseConfiguration, user);
    }
  }

  private final class XmlLocalDatabaseComponentImpl implements XmlLocalDatabaseComponent {
    private Provider<TransactionManager> transactionManagerProvider;

    private Provider<DatabaseConfiguration> databaseConfigurationProvider;

    private Provider<XmlResourceManagerComponent.Builder> resourceManagerBuilderProvider;

    private Provider<ResourceSessionFactory<XmlResourceSession>> resourceManagerFactoryProvider;

    private Provider<ResourceStore<XmlResourceSession>> xmlResourceManagerProvider;

    private Provider<Database<XmlResourceSession>> xmlDatabaseProvider;

    private Provider<User> userProvider;

    private Provider<DatabaseType> databaseTypeProvider;

    private XmlLocalDatabaseComponentImpl(DatabaseConfiguration databaseConfigurationParam,
        User userParam) {

      initialize(databaseConfigurationParam, userParam);
    }

    @SuppressWarnings("unchecked")
    private void initialize(final DatabaseConfiguration databaseConfigurationParam,
        final User userParam) {
      this.transactionManagerProvider = DoubleCheck.provider((Provider) TransactionManagerImpl_Factory.create());
      this.databaseConfigurationProvider = InstanceFactory.create(databaseConfigurationParam);
      this.resourceManagerBuilderProvider = new Provider<XmlResourceManagerComponent.Builder>() {
        @Override
        public XmlResourceManagerComponent.Builder get() {
          return new XmlResourceManagerComponentBuilder();
        }
      };
      this.resourceManagerFactoryProvider = DoubleCheck.provider(XmlLocalDatabaseModule_ResourceManagerFactoryFactory.create(resourceManagerBuilderProvider));
      this.xmlResourceManagerProvider = DoubleCheck.provider(XmlLocalDatabaseModule_XmlResourceManagerFactory.create(DaggerDatabaseManager.this.resourceManagersProvider, resourceManagerFactoryProvider));
      this.xmlDatabaseProvider = DoubleCheck.provider(XmlLocalDatabaseModule_XmlDatabaseFactory.create(transactionManagerProvider, databaseConfigurationProvider, DaggerDatabaseManager.this.databaseSessionsProvider, xmlResourceManagerProvider, DaggerDatabaseManager.this.writeLocksRegistryProvider, DaggerDatabaseManager.this.resourceManagersProvider));
      this.userProvider = InstanceFactory.create(userParam);
      this.databaseTypeProvider = DoubleCheck.provider(LocalDatabaseModule_DatabaseTypeFactory.create(databaseConfigurationProvider));
    }

    @Override
    public Database<XmlResourceSession> database() {
      return xmlDatabaseProvider.get();
    }

    @Override
    public XmlResourceManagerComponent.Builder resourceManagerBuilder() {
      return new XmlResourceManagerComponentBuilder();
    }

    private final class XmlResourceManagerComponentBuilder implements XmlResourceManagerComponent.Builder {
      private ResourceConfiguration resourceConfig;

      private BufferManager bufferManager;

      private Path resourceFile;

      @Override
      public XmlResourceManagerComponentBuilder resourceConfig(
          ResourceConfiguration resourceConfiguration) {
        this.resourceConfig = Preconditions.checkNotNull(resourceConfiguration);
        return this;
      }

      @Override
      public XmlResourceManagerComponentBuilder bufferManager(BufferManager bufferManager) {
        this.bufferManager = Preconditions.checkNotNull(bufferManager);
        return this;
      }

      @Override
      public XmlResourceManagerComponentBuilder resourceFile(Path resourceFile) {
        this.resourceFile = Preconditions.checkNotNull(resourceFile);
        return this;
      }

      @Override
      public XmlResourceManagerComponent build() {
        Preconditions.checkBuilderRequirement(resourceConfig, ResourceConfiguration.class);
        Preconditions.checkBuilderRequirement(bufferManager, BufferManager.class);
        Preconditions.checkBuilderRequirement(resourceFile, Path.class);
        return new XmlResourceManagerComponentImpl(resourceConfig, bufferManager, resourceFile);
      }
    }

    private final class XmlResourceManagerComponentImpl implements XmlResourceManagerComponent {
      private Provider<ResourceConfiguration> resourceConfigProvider;

      private Provider<BufferManager> bufferManagerProvider;

      private Provider<IOStorage> ioStorageProvider;

      private Provider<UberPage> rootPageProvider;

      private Provider<Semaphore> writeLockProvider;

      private Provider<PageTrxFactory> pageTrxFactoryProvider;

      private Provider<XmlResourceSessionImpl> xmlResourceSessionImplProvider;

      private Provider<XmlResourceSession> resourceSessionProvider;

      private XmlResourceManagerComponentImpl(ResourceConfiguration resourceConfigParam,
          BufferManager bufferManagerParam, Path resourceFile) {

        initialize(resourceConfigParam, bufferManagerParam, resourceFile);
      }

      @SuppressWarnings("unchecked")
      private void initialize(final ResourceConfiguration resourceConfigParam,
          final BufferManager bufferManagerParam, final Path resourceFile) {
        this.resourceConfigProvider = InstanceFactory.create(resourceConfigParam);
        this.bufferManagerProvider = InstanceFactory.create(bufferManagerParam);
        this.ioStorageProvider = DoubleCheck.provider(ResourceSessionModule_IoStorageFactory.create(resourceConfigProvider));
        this.rootPageProvider = DoubleCheck.provider(ResourceSessionModule_RootPageFactory.create(ioStorageProvider));
        this.writeLockProvider = DoubleCheck.provider(ResourceSessionModule_WriteLockFactory.create(DaggerDatabaseManager.this.writeLocksRegistryProvider, resourceConfigProvider));
        this.pageTrxFactoryProvider = PageTrxFactory_Factory.create(XmlLocalDatabaseComponentImpl.this.databaseTypeProvider);
        this.xmlResourceSessionImplProvider = XmlResourceSessionImpl_Factory.create(XmlLocalDatabaseComponentImpl.this.xmlResourceManagerProvider, resourceConfigProvider, bufferManagerProvider, ioStorageProvider, rootPageProvider, writeLockProvider, XmlLocalDatabaseComponentImpl.this.userProvider, pageTrxFactoryProvider);
        this.resourceSessionProvider = DoubleCheck.provider((Provider) xmlResourceSessionImplProvider);
      }

      @Override
      public XmlResourceSession resourceManager() {
        return resourceSessionProvider.get();
      }
    }
  }
}
