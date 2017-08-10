package org.sirix.access;

/** Determines if a log must be replayed or not. */
public enum Restore {
	/** Yes, it must be replayed. */
	YES,

	/** No, it must not be replayed. */
	NO
}
