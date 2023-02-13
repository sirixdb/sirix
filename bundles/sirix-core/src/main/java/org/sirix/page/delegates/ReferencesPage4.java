/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.page.delegates;

import com.google.common.base.MoreObjects;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.page.DeserializedReferencesPage4Tuple;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;
import org.sirix.page.interfaces.Page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to provide basic reference handling functionality.
 */
public final class ReferencesPage4 implements Page {

  /**
   * Page reference 1.
   */
  private final List<PageReference> references;

  /**
   * Page reference 4.
   */
  private final ShortList offsets;

  /**
   * Constructor to initialize instance.
   */
  public ReferencesPage4() {
    references = new ArrayList<>(4);
    offsets = new ShortArrayList(4);
  }

  /**
   * Constructor to initialize instance.
   *
   * @param in   input stream to read from
   * @param type the serialization type
   */
  public ReferencesPage4(final Bytes<?> in, final SerializationType type) {
    final DeserializedReferencesPage4Tuple tuple = type.deserializeReferencesPage4(in);
    references = tuple.references();
    offsets = tuple.offsets();
  }

  /**
   * Constructor to initialize instance.
   *
   * @param pageToClone committed page
   */
  public ReferencesPage4(final ReferencesPage4 pageToClone) {
    references = new ArrayList<>(4);
    offsets = new ShortArrayList(4);

    final var otherOffsets = pageToClone.getOffsets();

    for (int offset = 0, size = otherOffsets.size(); offset < size; offset++) {
      offsets.add(otherOffsets.getShort(offset));
      final var pageReference = new PageReference();
      final var pageReferenceToClone = pageToClone.getReferences().get(offset);
      pageReference.setKey(pageReferenceToClone.getKey());
      pageReference.setLogKey(pageReferenceToClone.getLogKey());
      pageReference.setPageFragments(pageReferenceToClone.getPageFragments());
      references.add(pageReference);
    }
  }

  public ShortList getOffsets() {
    return offsets;
  }

  @Override
  public List<PageReference> getReferences() {
    return references;
  }

  /**
   * Get page reference of given offset.
   *
   * @param offset offset of page reference
   * @return {@link PageReference} at given offset
   */
  @Override
  public PageReference getOrCreateReference(final @NonNegative int offset) {
    for (final var currOffset : offsets) {
      if (currOffset == offset) {
        return references.get(offset);
      }
    }

    if (offsets.size() < 4) {
      offsets.add((short) offset);
      final var newReference = new PageReference();
      references.add(newReference);
      return newReference;
    }

    return null;
  }

  @Override
  public boolean setOrCreateReference(final int offset, final PageReference pageReference) {
    for (int i = 0, count = offsets.size(); i < count; i++) {
      if (offsets.getShort(i) == offset) {
        references.set(i, pageReference);
        return false;
      }
    }

    if (offsets.size() < 4) {
      offsets.add((short) offset);
      references.add(pageReference);
      return false;
    }

    return true;
  }

  @Override
  public void serialize(final PageReadOnlyTrx pageReadOnlyTrx, final Bytes<ByteBuffer> out, final SerializationType type) {
    assert out != null;
    assert type != null;

    type.serializeReferencesPage4(out, references, offsets);
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (final int offset : offsets) {
      helper.add("offset", offset);
    }
    for (final PageReference ref : references) {
      helper.add("reference", ref);
    }
    return helper.toString();
  }

  @Override
  public Page clearPage() {
    offsets.clear();
    references.clear();
    return this;
  }
}
