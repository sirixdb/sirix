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
package org.sirix.diff.algorithm.fmse;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Stores whether two objects have a (unidirectional) connection. /!\ The identities of the objects
 * are used, not equals()!
 * 
 * @param <T> generic type for the map
 */
public final class ConnectionMap<T> {

  /**
   * First, we search the first node in the map, then the second in the returned map.
   */
  private final Map<T, HashMap<T, Boolean>> map;

  /**
   * Creates a new connection map.
   */
  public ConnectionMap() {
    map = new HashMap<>();
  }

  /**
   * Copy constructor.
   * 
   * @param map the original {@link ConnectionMap}
   */
  public ConnectionMap(final ConnectionMap<T> map) {
    this.map = new HashMap<>(map.map);
  }

  /**
   * Sets the connection between a and b.
   * 
   * @param origin origin object
   * @param destination destination object
   * @param bool if connection is established or not
   */
  public void set(final T origin, final T destination, final boolean bool) {
    requireNonNull(destination);
    if (!map.containsKey(requireNonNull(origin))) {
      map.put(origin, new HashMap<>());
    }
    map.get(origin).put(destination, bool);
  }

  /**
   * Returns whether there is a connection between a and b. Unknown objects do never have a
   * connection.
   * 
   * @param origin origin object
   * @param destination destination object
   * @return true, iff there is a connection from a to b
   */
  public boolean get(final T origin, final T destination) {
    if (!map.containsKey(origin)) {
      return false;
    }
    final Boolean bool = map.get(origin).get(destination);
    return bool != null && bool;
  }

  /** Reset datastructure. */
  public void reset() {
    map.clear();
  }
}
