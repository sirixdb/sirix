package org.sirix.xquery.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.xdm.AbstractTemporalNode;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Store;
import org.brackit.xquery.xdm.Stream;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.io.StorageType;
import org.sirix.service.xml.shredder.Insert;

import com.google.common.base.Optional;

/**
 * Database storage.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class DBStore implements Store, AutoCloseable {

	/** User home directory. */
	private static final String USER_HOME = System.getProperty("user.home");

	/** Storage for databases: Sirix data in home directory. */
	private static final File LOCATION = new File(USER_HOME, "sirix-data");

	/** Determines if collections have to be updating or not. */
	public enum Updating {
		/**
		 * Yes, collections generated with this BDStore instance should be
		 * updatable.
		 */
		YES,

		/**
		 * No, collections generated with this BDStore instance should be updatable.
		 */
		NO;
	}

	/** {@link Updating} value constant. */
	private Updating mUpdating;

	/** {@link Set} of databases. */
	private final Set<Database> mDatabases;

	/** {@link StorageType} instance. */
	private final StorageType mStorageType;

	/** The location to store created collections/databases. */
	private final File mLocation;

	/** Get a new builder instance. */
	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder setting up the store.
	 */
	public static class Builder {
		/** Determines if the databases created by the store should be updatable. */
		private boolean mIsUpdatable;

		/** Storage type. */
		private StorageType mStorageType = StorageType.FILE;

		/** The location to store created collections/databases. */
		private File mLocation = LOCATION;

		/**
		 * Determines if collections should be updatable.
		 * 
		 * @return this builder instance
		 */
		public Builder isUpdatable() {
			mIsUpdatable = true;
			return this;
		}

		/**
		 * Set the storage type (default: file backend).
		 * 
		 * @param storageType
		 *          storage type
		 * @return this builder instance
		 */
		public Builder storageType(final StorageType storageType) {
			mStorageType = checkNotNull(storageType);
			return this;
		}

		/**
		 * Set the location where to store the created databases/collections.
		 * 
		 * @param location
		 *          the location
		 * @return this builder instance
		 */
		public Builder location(final File location) {
			mLocation = checkNotNull(location);
			return this;
		}

		/**
		 * Create a new {@link DBStore} instance
		 * 
		 * @return new {@link DBStore} instance
		 */
		public DBStore build() {
			return new DBStore(this);
		}
	}

	/**
	 * Private constructor.
	 * 
	 * @param builder
	 *          builder instance
	 */
	private DBStore(final Builder builder) {
		mDatabases = new HashSet<>();
		mUpdating = builder.mIsUpdatable ? Updating.YES : Updating.NO;
		mStorageType = builder.mStorageType;
		mLocation = builder.mLocation;
	}

	/** Get the location of the generated collections/databases. */
	public File getLocation() {
		return mLocation;
	}

	@Override
	public Collection<?> lookup(final String name) throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				mLocation, name));
		if (Databases.existsDatabase(dbConf)) {
			try {
				final Database database = Databases.openDatabase(dbConf.getFile());
				mDatabases.add(database);
				return new DBCollection(name, database, mUpdating);
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public Collection<?> create(final String name) throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				mLocation, name));
		try {
			if (Databases.createDatabase(dbConf)) {
				throw new DocumentException("Document with name %s exists!", name);
			}

			final Database database = Databases.openDatabase(dbConf.getFile());
			mDatabases.add(database);
			return new DBCollection(name, database, mUpdating);
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public Collection<?> create(final String collName, final SubtreeParser parser)
			throws DocumentException {
		return create(collName, Optional.<String> absent(), parser);
	}

	public Collection<?> create(final String collName,
			final Optional<String> optResName, final SubtreeParser parser)
			throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				mLocation, collName));
		try {
			Databases.truncateDatabase(dbConf);
			Databases.createDatabase(dbConf);
			final Database database = Databases.openDatabase(dbConf.getFile());
			mDatabases.add(database);
			final String resName = optResName.isPresent() ? optResName.get()
					: new StringBuilder(3).append("resource")
							.append(database.listResources().length + 1).toString();
			database.createResource(ResourceConfiguration.newBuilder(resName, dbConf)
					.useDeweyIDs(true).useTextCompression(true).buildPathSummary(true)
					.storageType(mStorageType).build());
			final Session session = database
					.getSession(new SessionConfiguration.Builder(resName).build());
			final NodeWriteTrx wtx = session.beginNodeWriteTrx();

			final DBCollection collection = new DBCollection(collName, database,
					mUpdating);
			parser
					.parse(new SubtreeBuilder(
							collection,
							wtx,
							Insert.ASFIRSTCHILD,
							Collections
									.<SubtreeListener<? super AbstractTemporalNode<DBNode>>> emptyList()));
			wtx.commit();
			wtx.close();
			return collection;
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public Collection<?> create(final String collName,
			final @Nullable Stream<SubtreeParser> parsers) throws DocumentException {
		if (parsers != null) {
			final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
					mLocation, collName));
			try {
				Databases.truncateDatabase(dbConf);
				Databases.createDatabase(dbConf);
				final Database database = Databases.openDatabase(dbConf.getFile());
				mDatabases.add(database);
				final ExecutorService pool = Executors.newFixedThreadPool(Runtime
						.getRuntime().availableProcessors());
				int i = database.listResources().length + 1;
				try {
					SubtreeParser parser = null;
					while ((parser = parsers.next()) != null) {
						final SubtreeParser nextParser = parser;
						final String resource = new StringBuilder("resource").append(
								String.valueOf(i)).toString();
						pool.submit(new Callable<Void>() {
							@Override
							public Void call() throws DocumentException, SirixException {
								database.createResource(ResourceConfiguration
										.newBuilder(resource, dbConf).storageType(mStorageType)
										.useDeweyIDs(true).useTextCompression(true)
										.buildPathSummary(true).build());
								final Session session = database
										.getSession(new SessionConfiguration.Builder(resource)
												.build());
								final NodeWriteTrx wtx = session.beginNodeWriteTrx();
								final DBCollection collection = new DBCollection(collName,
										database, mUpdating);
								nextParser.parse(new SubtreeBuilder(
										collection,
										wtx,
										Insert.ASFIRSTCHILD,
										Collections
												.<SubtreeListener<? super AbstractTemporalNode<DBNode>>> emptyList()));
								wtx.commit();
								wtx.close();
								return null;
							}
						});
						i++;
					}
				} finally {
					parsers.close();
				}
				pool.shutdown();
				pool.awaitTermination(5, TimeUnit.MINUTES);
				return new DBCollection(collName, database, mUpdating);
			} catch (final SirixException | InterruptedException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public void drop(final String name) throws DocumentException {
		final DatabaseConfiguration dbConfig = new DatabaseConfiguration(new File(
				mLocation, name));
		if (Databases.existsDatabase(dbConfig)) {
			try {
				Databases.truncateDatabase(dbConfig);
			} catch (final SirixIOException e) {
				throw new DocumentException(e.getCause());
			}
		}
		throw new DocumentException("No collection with the specified name found!");
	}

	@Override
	public void makeDir(final String path) throws DocumentException {
		try {
			Files.createDirectory(java.nio.file.Paths.get(path));
		} catch (final IOException e) {
			throw new DocumentException(e.getCause());
		}
	}

	/**
	 * Commit all running write-transactions.
	 * 
	 * @throws DocumentException
	 *           if Sirix fails to commit a transaction
	 */
	public void commitAll() throws DocumentException {
		try {
			for (final Database database : mDatabases) {
				database.commitAll();
			}
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void close() throws DocumentException {
		try {
			for (final Database database : mDatabases) {
				database.close();
			}
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}
}
