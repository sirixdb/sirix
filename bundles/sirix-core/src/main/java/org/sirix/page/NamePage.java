/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.page;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.indexes.Names;
import org.sirix.io.ITTSink;
import org.sirix.io.ITTSource;
import org.sirix.node.ENode;
import org.sirix.page.interfaces.IPage;

/**
 * <h1>NamePageBinding</h1>
 * 
 * <p>
 * Name page holds all names and their keys for a revision.
 * </p>
 */
public final class NamePage implements IPage {

  /** Attribute names. */
  private final Names mAttributes;

  /** Element names. */
  private final Names mElements;

  /** Namespace URIs. */
  private final Names mNamespaces;
  
  /** Revision number. */
  private final long mRevision;

  /**
   * Create name page.
   * 
   * @param pRevision
   *          revision number
   */
  public NamePage(@Nonnegative final long pRevision) {
    checkArgument(pRevision >= 0, "pRevision must be >= 0!");
    mRevision = pRevision;
    mAttributes = Names.getInstance();
    mElements = Names.getInstance();
    mNamespaces = Names.getInstance();
  }

  /**
   * Read name page.
   * 
   * @param pIn
   *          input bytes to read from
   */
  protected NamePage(@Nonnull final ITTSource pIn) {
    mRevision = pIn.readLong();
    mElements = Names.clone(pIn);
    mNamespaces = Names.clone(pIn);
    mAttributes = Names.clone(pIn);
  }

  /**
   * Get raw name belonging to name key.
   * 
   * @param pKey
   *          name key identifying name
   * @return raw name of name key
   */
  public byte[] getRawName(final int pKey, final ENode pNodeKind) {
    byte[] rawName = new byte[] {};
    switch (pNodeKind) {
    case ELEMENT_KIND:
      rawName = mElements.getRawName(pKey);
      break;
    case NAMESPACE_KIND:
      rawName = mNamespaces.getRawName(pKey);
      break;
    case ATTRIBUTE_KIND:
      rawName = mAttributes.getRawName(pKey);
      break;
    default:
      throw new IllegalStateException("No other node types supported!");
    }
    return rawName;
  }

  /**
   * Get raw name belonging to name key.
   * 
   * @param pKey
   *          name key identifying name
   * @return raw name of name key
   */
  public String getName(final int pKey, final ENode pNodeKind) {
    String name;
    switch (pNodeKind) {
    case ELEMENT_KIND:
      name = mElements.getName(pKey);
      break;
    case NAMESPACE_KIND:
      name = mNamespaces.getName(pKey);
      break;
    case ATTRIBUTE_KIND:
      name = mAttributes.getName(pKey);
      break;
    default:
      throw new IllegalStateException("No other node types supported!");
    }
    return name;
  }

  /**
   * Create name key given a name.
   * 
   * @param pKey
   *          key for given name
   * @param pName
   *          name to create key for
   */
  public void setName(final int pKey, final String pName, final ENode pNodeKind) {
    switch (pNodeKind) {
    case ELEMENT_KIND:
      mElements.setName(pKey, pName);
      break;
    case NAMESPACE_KIND:
      mNamespaces.setName(pKey, pName);
      break;
    case ATTRIBUTE_KIND:
      mAttributes.setName(pKey, pName);
      break;
    default:
      throw new IllegalStateException("No other node types supported!");
    }
  }

  @Override
  public void serialize(final ITTSink pOut) {
    mElements.serialize(pOut);
    mNamespaces.serialize(pOut);
    mAttributes.serialize(pOut);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("revision", mRevision).add("elements", mElements).add(
      "attributes", mAttributes).add("URIs", mNamespaces).toString();
  }

  /**
   * Remove an attribute-name.
   * 
   * @param pKey
   *          the key to remove
   */
  public void removeName(final int pKey, final ENode pNodeKind) {
    switch (pNodeKind) {
    case ELEMENT_KIND:
      mElements.removeName(pKey);
      break;
    case NAMESPACE_KIND:
      mNamespaces.removeName(pKey);
      break;
    case ATTRIBUTE_KIND:
      mAttributes.removeName(pKey);
      break;
    default:
      throw new IllegalStateException("No other node types supported!");
    }
  }

  @Override
  public long getRevision() {
    return mRevision;
  }

  @Override
  public PageReference[] getReferences() {
    return null;
  }

  @Override
  public void commit(final IPageWriteTrx pPageWriteTrx) throws AbsTTException {
  }
}
