package org.treetank.diff.algorithm.fmse.utils;

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

/**
 * Package: costfunctions
 * Description: SubCost01 implements a substitution cost function where d(i,j) = 1 if idoes not equal j, 0 if
 * i equals j.
 * 
 * Date: 24-Mar-2004
 * Time: 13:38:12
 * 
 * @author Sam Chapman <a href="http://www.dcs.shef.ac.uk/~sam/">Website</a>, <a
 *         href="mailto:sam@dcs.shef.ac.uk">Email</a>.
 * @version 1.1
 */
public final class SubCost01 implements ISubstitutionCost {

  /**
   * Get the name of the cost function.
   * 
   * @return the name of the cost function
   */
  @Override
  public String getShortDescriptionString() {
    return "SubCost01";
  }

  /**
   * Cost between characters where d(i,j) = 1 if i does not equals j, 0 if i equals j.
   * 
   * @param str1
   *          the string1 to evaluate the cost
   * @param string1Index
   *          the index within the string1 to test
   * @param str2
   *          the string2 to evaluate the cost
   * @param string2Index
   *          the index within the string2 to test
   * @return the cost of a given subsitution d(i,j) where d(i,j) = 1 if i!=j, 0 if i==j
   */
  @Override
  public float getCost(final String str1, final int string1Index, final String str2, final int string2Index) {
    if (str1.charAt(string1Index) == str2.charAt(string2Index)) {
      return 0.0f;
    } else {
      return 1.0f;
    }
  }

  /**
   * Get the maximum possible cost.
   * 
   * @return the maximum possible cost
   */
  @Override
  public float getMaxCost() {
    return 1.0f;
  }

  /**
   * Get the minimum possible cost.
   * 
   * @return the minimum possible cost
   */
  @Override
  public float getMinCost() {
    return 0.0f;
  }
}
