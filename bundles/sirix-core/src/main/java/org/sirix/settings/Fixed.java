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

/**
 * Fixed constants for Sirix stuff. These constants should never be changed.
 * 
 * @author Sebastian Graf, University of Konstanz
 */
public enum Fixed {

  // --- File Version
  // ----------------------------------------------------------------
  /** Major version number of this release. */
  VERSION_MAJOR(5),

  /** Minor version number of this release. */
  VERSION_MINOR(1),

  /** Last major version to which this version is binary compatible. */
  LAST_VERSION_MAJOR(5),

  /** Last minor version to which this version is binary compatible. */
  LAST_VERSION_MINOR(1),

  // --- Keys
  // -------------------------------------------------------------
  /** Root node page key constant. */
  ROOT_PAGE_KEY(0L),

  /** Document node key constant. */
  DOCUMENT_NODE_KEY(0L),

  /** Null key for nodes. */
  NULL_NODE_KEY(-1L),

  /** Invalid key to validate if Json or Xml */
  INVALID_KEY_FOR_TYPE_CHECK(-2L);

  /**
   * Standard property.
   */
  private final long standardProperty;

  /**
   * Private constructor.
   * 
   * @param property property to set
   */
  private Fixed(final long property) {
    standardProperty = property;
  }

  /**
   * Getting the property.
   * 
   * @return the prop
   */
  public long getStandardProperty() {
    return standardProperty;
  }
}
