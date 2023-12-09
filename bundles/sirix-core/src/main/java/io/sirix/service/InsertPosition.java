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

package io.sirix.service;

/**
 * Determines where to insert a subtree.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public enum InsertPosition {
  /** Subtree should be added as first child of the current node. */
  AS_FIRST_CHILD("asFirstChild"),

  /** Subtree should be added as last child of the current node. */
  AS_LAST_CHILD("asLastChild"),

  /**
   * Subtree should be added as a right sibling of the current node. This is not possible when the
   * {@code IWriteTransaction} is on root node.
   */
  AS_RIGHT_SIBLING("asRightSibling"),

  /**
   * Subtree should be added as a left sibling of the current node. This is not possible when the
   * {@code IWriteTransaction} is on root node.
   */
  AS_LEFT_SIBLING("asLeftSibling");

  private final String name;

  InsertPosition(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static InsertPosition ofString(String name) {
    for (InsertPosition insertPosition : InsertPosition.values()) {
      if (insertPosition.getName().equalsIgnoreCase(name)) {
        return insertPosition;
      }
    }

    throw new IllegalArgumentException("No enum constant " + InsertPosition.class.getCanonicalName() + "." + name);
  }
}
