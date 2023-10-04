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

package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 *
 * <p>
 * This class acts as a container for revisioned {@link KeyValuePage}s. Each {@link KeyValuePage} is
 * stored in a versioned manner. If modifications occur, the versioned {@link KeyValuePage}s are
 * dereferenced and reconstructed. Afterwards, this container is used to store a complete
 * {@link KeyValuePage} as well as one for upcoming modifications.
 * </p>
 *
 * <p>
 * Both {@link KeyValuePage}s can differ since the complete one is mainly used for read access and
 * the modifying one for write access (and therefore mostly lazy dereferenced).
 * </p>
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PageContainer {

  /** {@link KeyValueLeafPage} reference, which references the complete key/value page. */
  private final Page complete;

  /** {@link KeyValueLeafPage} reference, which references the modified key/value page. */
  private final Page modified;

  /** Empty instance. */
  private static final PageContainer EMPTY_INSTANCE = new PageContainer(null, null);

  /**
   * Get the empty instance (parameterized).
   *
   * @return the empty instance
   */
  public static PageContainer emptyInstance() {
    return EMPTY_INSTANCE;
  }

  /**
   * Get a new instance.
   *
   * @param complete to be used as a base for this container
   * @param modifying to be used as a base for this container
   */
  public static PageContainer getInstance(final Page complete, final Page modifying) {
    // Assertions as it's not part of the public API.
    assert complete != null;
    assert modifying != null;
    return new PageContainer(complete, modifying);
  }

  /**
   * Private constructor with both, complete and modifying page.
   *
   * @param complete to be used as a base for this container
   * @param modified to be used as a base for this container
   */
  private PageContainer(final Page complete, final Page modified) {
    this.complete = complete;
    this.modified = modified;
  }

  /**
   * Getting the complete page.
   *
   * @return the complete page
   */
  public Page getComplete() {
    return complete;
  }

  /**
   * Getting the modified page.
   *
   * @return the modified page
   */
  public Page getModified() {
    return modified;
  }

  public KeyValueLeafPage getCompleteAsUnorderedKeyValuePage() {
    return (KeyValueLeafPage) complete;
  }

  public KeyValueLeafPage getModifiedAsUnorderedKeyValuePage() {
    return (KeyValueLeafPage) modified;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(complete, modified);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (!(obj instanceof PageContainer other))
      return false;

    return Objects.equal(complete, other.complete) && Objects.equal(modified, other.modified);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("complete page", complete)
                      .add("modified page", modified)
                      .toString();
  }
}
