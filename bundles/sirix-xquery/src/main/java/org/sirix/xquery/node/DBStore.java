package org.sirix.xquery.node;

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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.node.parser.SubtreeParser;
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
import org.sirix.service.xml.shredder.Insert;

/**
 * Database storage.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class DBStore implements Store, AutoCloseable {

	/** User home directory. */
	private static final String USER_HOME = System.getProperty("user.home");

	/** Storage for databases: Sirix data in home directory. */
	private static final File LOCATION = new File(USER_HOME, "sirix-data");

	/** Determines if collections have to be updating or not. */
	private boolean mUpdating;

	/** {@link Set} of databases. */
	private final Set<Database> mDatabases;

	/**
	 * Constructor.
	 */
	public DBStore() {
		mDatabases = new HashSet<>();
	}

	/** Get the location of the generated collections/databases. */
	public File getLocation() {
		return LOCATION;
	}

	/**
	 * Determines if collections have to be updatable.
	 * 
	 * @param updating
	 *          {@code true} if they should be updatable, {@code false} otherwise
	 */
	public DBStore isUpdating(final boolean updating) {
		mUpdating = updating;
		return this;
	}

	@Override
	public Collection<?> lookup(final @Nonnull String name)
			throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				LOCATION, name));
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
	public Collection<?> create(final @Nonnull String name)
			throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				LOCATION, name));
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
	public Collection<?> create(final @Nonnull String name,
			final @Nonnull SubtreeParser parser) throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				LOCATION, name));
		try {
			Databases.truncateDatabase(dbConf);
			Databases.createDatabase(dbConf);
			final Database database = Databases.openDatabase(dbConf.getFile());
			mDatabases.add(database);
			database.createResource(new ResourceConfiguration.Builder("shredded",
					dbConf).useDeweyIDs(true).build());
			final Session session = database
					.getSession(new SessionConfiguration.Builder("shredded").build());
			final NodeWriteTrx wtx = session.beginNodeWriteTrx();

			final DBCollection collection = new DBCollection(name,
					database, mUpdating);
			parser.parse(new SubtreeBuilder(collection, wtx,
					Insert.ASFIRSTCHILD, Collections
							.<SubtreeListener<? super AbstractTemporalNode>> emptyList()));
			wtx.commit();
			wtx.close();
			return collection;
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public Collection<?> create(final @Nonnull String name,
			final @Nullable Stream<SubtreeParser> pParsers) throws DocumentException {
		if (pParsers != null) {
			final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
					LOCATION, name));
			try {
				Databases.truncateDatabase(dbConf);
				Databases.createDatabase(dbConf);
				final Database database = Databases.openDatabase(dbConf.getFile());
				mDatabases.add(database);
				final ExecutorService pool = Executors.newFixedThreadPool(Runtime
						.getRuntime().availableProcessors());
				try {
					SubtreeParser parser = null;
					int i = 0;
					while ((parser = pParsers.next()) != null) {
						final SubtreeParser nextParser = parser;
						final String resource = new StringBuilder("shredded").append(
								String.valueOf(i)).toString();
						pool.submit(new Callable<Void>() {
							@Override
							public Void call() throws DocumentException, SirixException {
								database.createResource(new ResourceConfiguration.Builder(
										resource, dbConf).useDeweyIDs(true).build());
								final Session session = database
										.getSession(new SessionConfiguration.Builder(resource)
												.build());
								final NodeWriteTrx wtx = session.beginNodeWriteTrx();
								final DBCollection collection = new DBCollection(
										name, database, mUpdating);
								nextParser.parse(new SubtreeBuilder(collection, wtx,
										Insert.ASFIRSTCHILD, Collections
												.<SubtreeListener<? super AbstractTemporalNode>> emptyList()));
								wtx.commit();
								wtx.close();
								return null;
							}
						});
						i++;
					}
				} finally {
					pParsers.close();
				}
				pool.shutdown();
				pool.awaitTermination(5, TimeUnit.MINUTES);
				return new DBCollection(name, database, mUpdating);
			} catch (final SirixException | InterruptedException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public void drop(final @Nonnull String name) throws DocumentException {
		final DatabaseConfiguration dbConfig = new DatabaseConfiguration(new File(
				LOCATION, name));
		if (Databases.existsDatabase(dbConfig)) {
			try {
				Databases.truncateDatabase(dbConfig);
			} catch (final SirixIOException e) {
				throw new DocumentException(e.getCause());
			}
		}
		throw new DocumentException();
	}

	@Override
	public void makeDir(final @Nonnull String path) throws DocumentException {
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
