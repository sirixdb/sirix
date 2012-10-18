package org.sirix.encryption;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * This class represents the key manager model holding all key data for a user
 * comprising the initial keys and all TEKs a user owns.
 * 
 * @author Patrick Lang, University of Konstanz
 */
@Entity
public class KeyManager {

	/**
	 * User and primary key for database.
	 */
	@PrimaryKey
	private String mUser;

	/**
	 * Initial trace of keys the user gets for entry a group.
	 */
	private Map<Long, List<Long>> mInitialKeys;

	/**
	 * List of all TEKs the user owns.
	 */
	private List<Long> mTekKeys;

	/**
	 * Standard constructor.
	 */
	public KeyManager() {
		super();
	}

	/**
	 * Constructor for building an new key manager instance.
	 * 
	 * @param paramUser
	 *          user.
	 * @param paramInitial
	 *          map of all key trails.
	 */
	public KeyManager(final String paramUser,
			final Map<Long, List<Long>> paramInitial) {
		this.mUser = paramUser;
		this.mInitialKeys = paramInitial;
		this.mTekKeys = new LinkedList<Long>();

		final Iterator iter = paramInitial.keySet().iterator();
		while (iter.hasNext()) {
			final long mMapKey = (Long) iter.next();
			final List<Long> mKeyTrail = paramInitial.get(mMapKey);
			final int mKeyTrailSize = mKeyTrail.size() - 1;
			final long mTek = mKeyTrail.get(mKeyTrailSize);
			mTekKeys.add(mTek);
			// break after first iteration since
			// tek is identical to all key trails
			break;
		}
	}

	/**
	 * Returns a user.
	 * 
	 * @return user.
	 */
	public final String getUser() {
		return mUser;
	}

	/**
	 * Returns a list of initial keys.
	 * 
	 * @return list of initial keys.
	 */
	public final Map<Long, List<Long>> getInitialKeys() {
		return mInitialKeys;
	}

	/**
	 * Add a new key trail to the map.
	 * 
	 * @param paramTrail
	 *          new key trail.
	 */
	public final void addInitialKeyTrail(final List<Long> paramTrail) {
		mInitialKeys.put(paramTrail.get(0), paramTrail);
	}

	public final void removeInitialKeyTrail(final long paramKey) {
		mInitialKeys.remove(paramKey);
	}

	/**
	 * Returns a list of TEKs the user owns.
	 * 
	 * @return TEK list.
	 */
	public final List<Long> getTEKs() {
		return mTekKeys;
	}

	/**
	 * Adds a new TEK to users TEK list.
	 * 
	 * @param paramTek
	 *          new TEK to add.
	 */
	public final void addTEK(final long paramTek) {
		mTekKeys.add(paramTek);
	}

}
