/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.settings;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Interface to hold all constants of the node layer.
 */
public final class Constants {

  /**
   * Private constructor.
   */
  private Constants() {
    // Cannot be instantiated.
    throw new AssertionError("May not be instantiated!");
  }

  // --- Varia
  // ------------------------------------------------------------------

  /** Default internal encoding. */
  public static final Charset DEFAULT_ENCODING = StandardCharsets.UTF_8;

  // --- Indirect Page
  // ----------------------------------------------------------

  /** Count of indirect references in indirect page. */
  public static final int INP_REFERENCE_COUNT = 1024;

  /** 2^INP_REFERENCE_COUNT_EXPONENT = INP_REFERENCE_COUNT. */
  public static final int INP_REFERENCE_COUNT_EXPONENT = 10;

  /**
   * Exponent of pages per level (root level = 0, leaf level = 7). 2 ^ (7 *
   * INP_REFERENCE_COUNT_EXPONENT) = INP_REFERENCE_COUNT ^ 7, 2 ^ (6 * INP_REFERENCE_COUNT_EXPONENT)
   * = INP_REFERENCE_COUNT ^ 6....
   */
  public static final int[] INP_LEVEL_PAGE_COUNT_EXPONENT = {
      7 * INP_REFERENCE_COUNT_EXPONENT, 6 * INP_REFERENCE_COUNT_EXPONENT,
      5 * INP_REFERENCE_COUNT_EXPONENT,
      4 * INP_REFERENCE_COUNT_EXPONENT, 3 * INP_REFERENCE_COUNT_EXPONENT,
      2 * INP_REFERENCE_COUNT_EXPONENT, 1 * INP_REFERENCE_COUNT_EXPONENT,
      0 * INP_REFERENCE_COUNT_EXPONENT};

  // --- Path summary
  // -------------------------------------------------------------

  /** Count of indirect references in indirect page. */
  public static final int PATHINP_REFERENCE_COUNT = 1024;

  /** 2^PATHINP_REFERENCE_COUNT_EXPONENT = PATHINP_REFERENCE_COUNT. */
  public static final int PATHINP_REFERENCE_COUNT_EXPONENT = 10;

  /**
   * Exponent of pages per level (root level = 0, leaf level = 7). 2 ^ (7 *
   * PATHINP_REFERENCE_COUNT_EXPONENT) = PATHINP_REFERENCE_COUNT ^ 7, 2 ^ (6 *
   * PATHINP_REFERENCE_COUNT_EXPONENT) = PATHINP_REFERENCE_COUNT ^ 6....
   */
  public static final int[] PATHINP_LEVEL_PAGE_COUNT_EXPONENT = {
      4 * PATHINP_REFERENCE_COUNT_EXPONENT,
      3 * PATHINP_REFERENCE_COUNT_EXPONENT,
      2 * PATHINP_REFERENCE_COUNT_EXPONENT,
      1 * PATHINP_REFERENCE_COUNT_EXPONENT, 0 * PATHINP_REFERENCE_COUNT_EXPONENT};

  // --- Uber Page
  // -------------------------------------------------------------

  /** Count of indirect references in indirect page. */
  public static final int UBPINP_REFERENCE_COUNT = 1024;

  /** 2^INP_REFERENCE_COUNT_EXPONENT = INP_REFERENCE_COUNT. */
  public static final int UBPINP_REFERENCE_COUNT_EXPONENT = 10;

  /**
   * Exponent of pages per level (root level = 0, leaf level = 7). 2 ^ (7 *
   * INP_REFERENCE_COUNT_EXPONENT) = INP_REFERENCE_COUNT ^ 7, 2 ^ (6 * INP_REFERENCE_COUNT_EXPONENT)
   * = INP_REFERENCE_COUNT ^ 6....
   */
  public static final int[] UBPINP_LEVEL_PAGE_COUNT_EXPONENT = {
      4 * UBPINP_REFERENCE_COUNT_EXPONENT,
      3 * UBPINP_REFERENCE_COUNT_EXPONENT,
      2 * UBPINP_REFERENCE_COUNT_EXPONENT, 1 * UBPINP_REFERENCE_COUNT_EXPONENT,
      0 * UBPINP_REFERENCE_COUNT_EXPONENT};

  /** Revision count of uninitialized storage. */
  public static final int UBP_ROOT_REVISION_COUNT = 1;

  /** Root revisionKey guaranteed to exist in empty storage. */
  public static final int UBP_ROOT_REVISION_NUMBER = 0;

  // --- Node Page
  // -------------------------------------------------------------

  /** Maximum node count per node page. */
  public static final int NDP_NODE_COUNT = 1024;

  /** 2^NDP_NODE_COUNT_EXPONENT = NDP_NODE_COUNT. */
  public static final int NDP_NODE_COUNT_EXPONENT = 10;

  // --- Reference Page
  // -------------------------------------------------------------

  public static final long NULL_ID_LONG = -15;

  public static final int NULL_ID_INT = -15;
}
