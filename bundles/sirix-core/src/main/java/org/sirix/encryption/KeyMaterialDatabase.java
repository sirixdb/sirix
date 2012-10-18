package org.sirix.encryption;

import java.io.File;
import java.util.SortedMap;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

import org.sirix.access.AbstractKeyDatabase;
import org.sirix.exception.SirixIOException;

/**
 * Berkeley implementation of a persistent keying material database. That means
 * that all data is stored in this database and it is never removed.
 * 
 * @author Patrick Lang, University of Konstanz
 */
public class KeyMaterialDatabase extends AbstractKeyDatabase {

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
	private static final String NAME = "berkeleyKeyMaterial";

	/**
	 * Constructor. Building up the berkeley db and setting necessary settings.
	 * 
	 * @param paramFile
	 *          the place where the berkeley db is stored.
	 */
	public KeyMaterialDatabase(final File paramFile) {
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
	 * Putting a {@link KeyMaterial} into the database with a corresponding key.
	 * 
	 * @param paramSelect
	 *          selector instance to get information of node.
	 * @return generated unique material key of new keying material.
	 */
	public final long putPersistent(final KeyingMaterial paramMat) {

		PrimaryIndex<Long, KeyingMaterial> primaryIndex;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeyingMaterial.class);

			primaryIndex.put(paramMat);
		} catch (final DatabaseException e) {
			e.printStackTrace();
		}
		return paramMat.getMaterialKey();

	}

	/**
	 * Getting a {@link KeyingMaterial} related to a given material key.
	 * 
	 * @param paramKey
	 *          material key for related keying material.
	 * @return keying material instance.
	 */
	public final KeyingMaterial getPersistent(final long paramKey) {
		PrimaryIndex<Long, KeyingMaterial> primaryIndex;
		KeyingMaterial entity = null;
		try {
			primaryIndex =

			mStore.getPrimaryIndex(Long.class, KeyingMaterial.class);
			entity = primaryIndex.get(paramKey);

		} catch (final DatabaseException mDbExp) {
			mDbExp.printStackTrace();
		}
		return entity;
	}

	/**
	 * Deletes an entry from storage.
	 * 
	 * @param paramKey
	 *          primary key of entry to delete.
	 * @return status whether deletion was successful or not.
	 */
	public final boolean deleteEntry(final long paramKey) {
		PrimaryIndex<Long, KeyingMaterial> primaryIndex;
		boolean status = false;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeyingMaterial.class);
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
		PrimaryIndex<Long, KeyingMaterial> primaryIndex;
		long counter = 0;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeyingMaterial.class);
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
	public final SortedMap<Long, KeyingMaterial> getEntries() {
		PrimaryIndex<Long, KeyingMaterial> primaryIndex;
		SortedMap<Long, KeyingMaterial> sMap = null;
		try {
			primaryIndex = mStore.getPrimaryIndex(Long.class, KeyingMaterial.class);
			sMap = primaryIndex.sortedMap();

		} catch (final DatabaseException mDbExp) {
			mDbExp.printStackTrace();
		}
		return sMap;
	}

}
