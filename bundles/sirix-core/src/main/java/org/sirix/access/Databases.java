package org.sirix.access;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.utils.Files;

/**
 * Utility methods for {@link Database} handling.
 * 
 * @author Johannes Lichtenberger
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class Databases {

	/** Central repository of all running databases. */
	private static final ConcurrentMap<File, Database> DATABASEMAP = new ConcurrentHashMap<>();

	/**
	 * Creating a database. This includes loading the database configuration,
	 * building up the structure and preparing everything for login.
	 * 
	 * @param dbConfig
	 *          config which is used for the database, including storage location
	 * @return true if creation is valid, false otherwise
	 * @throws SirixIOException
	 *           if something odd happens within the creation process.
	 */
	public static synchronized boolean createDatabase(
			final DatabaseConfiguration dbConfig) throws SirixIOException {
		boolean returnVal = true;
		// if file is existing, skipping
		if (dbConfig.getFile().exists()) {
			return false;
		} else {
			returnVal = dbConfig.getFile().mkdirs();
			if (returnVal) {
				// creation of folder structure
				for (DatabaseConfiguration.Paths paths : DatabaseConfiguration.Paths
						.values()) {
					final File toCreate = new File(dbConfig.getFile(), paths.getFile()
							.getName());
					if (paths.isFolder()) {
						returnVal = toCreate.mkdir();
					} else {
						try {
							returnVal = toCreate.getName().equals(
									DatabaseConfiguration.Paths.LOCK.getFile().getName()) ? true
									: toCreate.createNewFile();
						} catch (final IOException e) {
							Files.recursiveRemove(dbConfig.getFile().toPath());
							throw new SirixIOException(e);
						}
					}
					if (!returnVal) {
						break;
					}
				}
			}
			// serialization of the config
			DatabaseConfiguration.serialize(dbConfig);

			// if something was not correct, delete the partly created
			// substructure
			if (!returnVal) {
				Files.recursiveRemove(dbConfig.getFile().toPath());
			}
			return returnVal;
		}
	}

	/**
	 * Truncate a database. This deletes all relevant data. All running sessions
	 * must be closed beforehand.
	 * 
	 * @param dbConfig
	 *          the database at this path should be deleted
	 * @throws SirixException
	 *           if Sirix fails to delete the database
	 */
	public static synchronized void truncateDatabase(
			final DatabaseConfiguration dbConfig) throws SirixIOException {
		// check that database must be closed beforehand
		if (!DATABASEMAP.containsKey(dbConfig.getFile())) {
			// if file is existing and folder is a tt-dataplace, delete it
			if (dbConfig.getFile().exists()) {
				// && DatabaseConfiguration.Paths.compareStructure(pConf.getFile()) ==
				// 0) {
				// instantiate the database for deletion
				Files.recursiveRemove(dbConfig.getFile().toPath());
			}
		}
	}

	/**
	 * Open database. A database can be opened only once (even across JVMs).
	 * Afterwards a singleton instance bound to the {@link File} is returned.
	 * 
	 * @param file
	 *          determines where the database is located sessionConf a
	 *          {@link SessionConfiguration} object to set up the session
	 * @return {@link Database} instance.
	 * @throws SirixException
	 *           if something odd happens
	 * @throws NullPointerException
	 *           if {@code pFile} is {@code null}
	 */
	public static synchronized Database openDatabase(final File file)
			throws SirixException {
		if (!file.exists()) {
			throw new SirixUsageException(
					"DB could not be opened (since it was not created?) at location",
					file.toString());
		}
		final DatabaseConfiguration config = DatabaseConfiguration
				.deserialize(file);
		if (config == null) {
			throw new IllegalStateException("Configuration may not be null!");
		}
		final File lock = new File(file, DatabaseConfiguration.Paths.LOCK.getFile()
				.getName());
		final Database database = new DatabaseImpl(config);
		if (lock.exists() && Databases.getDatabase(file) == null) {
			throw new SirixUsageException(
					"DB could not be opened (since it is in use by another JVM)",
					file.toString());
		} else {
			try {
				lock.createNewFile();
			} catch (final IOException e) {
				throw new SirixIOException(e.getCause());
			}
		}
		final Database returnVal = Databases.putDatabase(file, database);
		if (returnVal == null) {
			return database;
		} else {
			return returnVal;
		}
	}

	/**
	 * Determines if a database already exists.
	 * 
	 * @param dbConfig
	 *          database configuration
	 * @return {@code true}, if database exists, {@code false} otherwise
	 */
	public static synchronized boolean existsDatabase(
			final DatabaseConfiguration dbConfig) {
		return dbConfig.getFile().exists()
				&& DatabaseConfiguration.Paths.compareStructure(dbConfig.getFile()) == 0 ? true
				: false;
	}

	/**
	 * Package private method to get a database for a file.
	 * 
	 * @param file
	 *          database file to lookup
	 * @return the database handle associated with the file or {@code null} if no
	 *         database handle has been opened before for the specified file
	 */
	static synchronized Database getDatabase(final File file) {
		return DATABASEMAP.get(file);
	}

	/**
	 * Package private method to put a file/database into the internal map.
	 * 
	 * @param file
	 *          database file to put into the map
	 * @param database
	 *          database handle to put into the map
	 * @return the database handle associated with the file or {@code null} if no
	 *         database handle has been opened before for the specified file
	 */
	static synchronized Database putDatabase(final File file,
			final Database database) {
		return DATABASEMAP.putIfAbsent(file, database);
	}

	/**
	 * Package private method to remove a databse.
	 * 
	 * @param file
	 *          database file to remove
	 */
	static synchronized void removeDatabase(final File file) {
		DATABASEMAP.remove(file);
	}
}
