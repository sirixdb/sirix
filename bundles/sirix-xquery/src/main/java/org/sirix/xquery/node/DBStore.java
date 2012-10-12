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

import javax.annotation.Nullable;

import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Store;
import org.brackit.xquery.xdm.Stream;
import org.sirix.access.Database;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.ISession;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.service.xml.shredder.EInsert;

/**
 * Database storage.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class DBStore implements Store, AutoCloseable {

	/** User home directory. */
	private final String mUserHome = System.getProperty("user.home");

	/** Storage for databases: Sirix data in home directory. */
	private final File mLocation = new File(mUserHome, "sirix-data");

	/** Determines if collections have to be updating or not. */
	private boolean mUpdating;

	/** {@link Set} of databases. */
	private final Set<IDatabase> mDatabases;

	/**
	 * Constructor.
	 */
	public DBStore() {
		mDatabases = new HashSet<>();
	}

	/** Get the location of the generated collections/databases. */
	public File getLocation() {
		return mLocation;
	}

	/**
	 * Determines if collections have to be updatable.
	 * 
	 * @param pUpdating
	 *          {@code true} if they should be updatable, {@code false} otherwise
	 */
	public DBStore isUpdating(final boolean pUpdating) {
		mUpdating = pUpdating;
		return this;
	}

	@Override
	public Collection<?> lookup(final String pName) throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				mLocation, pName));
		if (Database.existsDatabase(dbConf)) {
			try {
				final IDatabase database = Database.openDatabase(dbConf.getFile());
				mDatabases.add(database);
				return new DBCollection<AbsTemporalNode>(pName, database, mUpdating);
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public Collection<?> create(final String pName) throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				mLocation, pName));
		try {
			if (Database.createDatabase(dbConf)) {
				throw new DocumentException("Document with name %s exists!", pName);
			}

			final IDatabase database = Database.openDatabase(dbConf.getFile());
			mDatabases.add(database);
			return new DBCollection<AbsTemporalNode>(pName, database, mUpdating);
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public Collection<?> create(final String pName, final SubtreeParser pParser)
			throws DocumentException {
		final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
				mLocation, pName));
		try {
			Database.truncateDatabase(dbConf);
			Database.createDatabase(dbConf);
			final IDatabase database = Database.openDatabase(dbConf.getFile());
			mDatabases.add(database);
			database.createResource(new ResourceConfiguration.Builder("shredded",
					dbConf).build());
			final ISession session = database
					.getSession(new SessionConfiguration.Builder("shredded").build());
			final INodeWriteTrx wtx = session.beginNodeWriteTrx();

			final DBCollection<DBNode> collection = new DBCollection<DBNode>(pName,
					database, mUpdating);
			pParser.parse(new SubtreeBuilder<DBNode>(collection, wtx,
					EInsert.ASFIRSTCHILD, Collections
							.<SubtreeListener<? super AbsTemporalNode>> emptyList()));
			wtx.commit();
			wtx.close();
			return collection;
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public Collection<?> create(final String pName,
			final @Nullable Stream<SubtreeParser> pParsers) throws DocumentException {
		if (pParsers != null) {
			final DatabaseConfiguration dbConf = new DatabaseConfiguration(new File(
					mLocation, pName));
			try {
				Database.truncateDatabase(dbConf);
				Database.createDatabase(dbConf);
				final IDatabase database = Database.openDatabase(dbConf.getFile());
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
										resource, dbConf).build());
								final ISession session = database
										.getSession(new SessionConfiguration.Builder(resource)
												.build());
								final INodeWriteTrx wtx = session.beginNodeWriteTrx();
								final DBCollection<DBNode> collection = new DBCollection<DBNode>(
										pName, database, mUpdating);
								nextParser.parse(new SubtreeBuilder<DBNode>(collection, wtx,
										EInsert.ASFIRSTCHILD, Collections
												.<SubtreeListener<? super AbsTemporalNode>> emptyList()));
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
				return new DBCollection<>(pName, database, mUpdating);
			} catch (final SirixException | InterruptedException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return null;
	}

	@Override
	public void drop(final String pName) throws DocumentException {
		final DatabaseConfiguration dbConfig = new DatabaseConfiguration(new File(
				mLocation, pName));
		if (Database.existsDatabase(dbConfig)) {
			try {
				Database.truncateDatabase(dbConfig);
			} catch (final SirixIOException e) {
				throw new DocumentException(e.getCause());
			}
		}
		throw new DocumentException();
	}

	@Override
	public void makeDir(final String pPath) throws DocumentException {
		try {
			Files.createDirectory(java.nio.file.Paths.get(pPath));
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
			for (final IDatabase database : mDatabases) {
				database.commitAll();
			}
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void close() throws DocumentException {
		try {
			for (final IDatabase database : mDatabases) {
				database.close();
			}
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}
}
