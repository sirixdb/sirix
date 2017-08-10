package org.sirix.encryption;

import java.io.File;
import java.util.SortedMap;

import org.sirix.access.AbstractKeyDatabase;
import org.sirix.exception.SirixIOException;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

/**
 * Berkeley implementation of a persistent key selector database. That means that all data is stored
 * in this database and it is never removed.
 * 
 * @author Patrick Lang, University of Konstanz
 */
public class KeySelectorDatabase extends AbstractKeyDatabase {

	/**
	 * Berkeley Environment for the database.
	 */
	private Environment mEnv;

	/**
	 * Berkeley Entity store instance for the database.
	 */
	private EntityStore mStore;

	/**
	 * Name for the database.
	 */
	private static final String NAME = "berkeleyKeySelector";

	/**
	 * Constructor. Building up the berkeley db and setting necessary settings.
	 * 
	 * @param paramFile the place where the berkeley db is stored.
	 */
	public KeySelectorDatabase(final File paramFile) {
		super(paramFile);
		EnvironmentConfig environmentConfig = new EnvironmentConfig();
		environmentConfig.setAllowCreate(true);
		environmentConfig.setTransactional(true);

		final DatabaseConfig conf = new DatabaseConfig();
		conf.setTransactional(true);
		conf.setKeyPrefixing(true);

		try {
			mEnv = new Environment(place, environmentConfig);

			StoreConfig storeConfig = new StoreConfig();
			storeConfig.setAllowCreate(true);
			storeConfig.setTransactional(true);
			mStore = new EntityStore(mEnv, NAME, storeConfig);

		} catch (final EnvironmentLockedException mELExp) {
			mELExp.printStackTrace();
		} catch (final DatabaseException mDbExp) {
			mDbExp.printStackTrace();
		}
	}

	/**
	 * Clearing the database. That is removing all elements
	 */
	public final void clearPersistent() {
		try {
			for (final File file : place.listFiles()) {
				if (!file.delete()) {
					throw new SirixIOException("Couldn't delete!");
				}
			}
			if (!place.delete()) {
				throw new SirixIOException("Couldn't delete!");
			}
			if (mStore != null) {
				mStore.close();
			}
			if (mEnv != null) {
				mEnv.close();
			}
		} catch (final DatabaseException mDbExp) {
			mDbExp.printStackTrace();
		} catch (final SirixIOException exc) {
			throw new IllegalStateException(exc);
		}

	}

	/**
	 * Putting a {@link KeySelector} into the database with a corresponding selector key.
	 * 
	 * @param entity key selector instance to put into database.
	 */
	public final void putPersistent(final KeySelector entity) {
		PrimaryIndex<Long, KeySelector> primaryIndex;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeySelector.class);

			primaryIndex.put(entity);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Getting a {@link KeyingSelector} related to a given selector key.
	 * 
	 * @param paramKey selector key for related key selector instance.
	 * @return key selector instance.
	 */
	public final KeySelector getPersistent(final long paramKey) {
		PrimaryIndex<Long, KeySelector> primaryIndex;
		KeySelector entity = null;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeySelector.class);
			entity = primaryIndex.get(paramKey);

		} catch (final DatabaseException mDbExp) {
			mDbExp.printStackTrace();
		}
		return entity;
	}

	/**
	 * Deletes an entry from storage.
	 * 
	 * @param paramKey primary key of entry to delete.
	 * @return status whether deletion was successful or not.
	 */
	public final boolean deleteEntry(final long paramKey) {
		PrimaryIndex<Long, KeySelector> primaryIndex;
		boolean status = false;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeySelector.class);
			status = primaryIndex.delete(paramKey);

		} catch (final DatabaseException mDbExp) {
			mDbExp.printStackTrace();
		}

		return status;
	}

	/**
	 * Returns number of database entries.
	 * 
	 * @return number of entries in database.
	 */
	public final int count() {
		PrimaryIndex<Long, KeySelector> primaryIndex;
		long counter = 0;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeySelector.class);
			counter = primaryIndex.count();

		} catch (final DatabaseException mDbExp) {
			mDbExp.printStackTrace();
		}
		return (int) counter;
	}

	/**
	 * Returns all database entries as {@link SortedMap}.
	 * 
	 * @return all database entries.
	 */
	public final SortedMap<Long, KeySelector> getEntries() {
		PrimaryIndex<Long, KeySelector> primaryIndex;
		SortedMap<Long, KeySelector> sMap = null;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeySelector.class);
			sMap = primaryIndex.sortedMap();

		} catch (final DatabaseException mDbExp) {
			mDbExp.printStackTrace();
		}
		return sMap;
	}

}
