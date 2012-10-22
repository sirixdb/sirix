package org.sirix.node.interfaces;

/**
 * Combines some basic commonalities for different DeweyID representations (as
 * for instance the {@link SirixDeweyID} and the DeweyIDBuffer).
 * 
 * @author Martin Hiller
 * 
 */
public interface SimpleDeweyID {
	/**
	 * Get the division values.
	 * 
	 * @return an array of divisions representing the DeweyID
	 */
	public int[] getDivisionValues();

	/**
	 * Get the number of divisions.
	 * 
	 * @return the number of used divisions (is less or equal to the length of the
	 *         divisions array)
	 */
	public int getNumberOfDivisions();

	/**
	 * Determines if it is an attribute.
	 * 
	 * @return {@code true}, iff this DeweyID represents an attribute node,
	 *         {@code false} otherwise
	 */
	public boolean isAttribute();
}