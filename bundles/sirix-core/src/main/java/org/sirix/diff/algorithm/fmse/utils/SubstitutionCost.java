/**
 * SimMetrics - SimMetrics is a java library of Similarity or Distance
 * Metrics, e.g. Levenshtein Distance, that provide float based similarity
 * measures between String Data. All metrics return consistant measures
 * rather than unbounded similarity scores.
 * 
 * Copyright (C) 2005 Sam Chapman - Open Source Release v1.1
 * 
 * Please Feel free to contact me about this library, I would appreciate
 * knowing quickly what you wish to use it for and any criticisms/comments
 * upon the SimMetric library.
 * 
 * email: s.chapman@dcs.shef.ac.uk
 * www: http://www.dcs.shef.ac.uk/~sam/
 * www: http://www.dcs.shef.ac.uk/~sam/stringmetrics.html
 * 
 * address: Sam Chapman,
 * Department of Computer Science,
 * University of Sheffield,
 * Sheffield,
 * S. Yorks,
 * S1 4DP
 * United Kingdom,
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

package org.sirix.diff.algorithm.fmse.utils;

/**
 * Package: costfunctions Description: InterfaceSubstitutionCost is an interface
 * for a cost function d(i,j). Date: 24-Mar-2004 Time: 13:26:21
 * 
 * @author Sam Chapman <a href="http://www.dcs.shef.ac.uk/~sam/">Website</a>, <a
 *         href="mailto:sam@dcs.shef.ac.uk">Email</a>.
 * @version 1.1
 */
public interface SubstitutionCost {

	/**
	 * returns the name of the cost function.
	 * 
	 * @return the name of the cost function
	 */
	public String getShortDescriptionString();

	/**
	 * get cost between characters.
	 * 
	 * @param str1
	 *          - the string1 to evaluate the cost
	 * @param string1Index
	 *          - the index within the string1 to test
	 * @param str2
	 *          - the string2 to evaluate the cost
	 * @param string2Index
	 *          - the index within the string2 to test
	 * 
	 * @return the cost of a given subsitution d(i,j)
	 */
	public float getCost(String str1, int string1Index, String str2,
			int string2Index);

	/**
	 * returns the maximum possible cost.
	 * 
	 * @return the maximum possible cost
	 */
	public float getMaxCost();

	/**
	 * returns the minimum possible cost.
	 * 
	 * @return the minimum possible cost
	 */
	public float getMinCost();
}
