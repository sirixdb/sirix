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
package org.sirix.node.delegates;

import java.util.Arrays;
import java.util.zip.Deflater;
import javax.annotation.Nullable;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.settings.Constants;
import org.sirix.utils.Compression;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Delegate method for all nodes containing \"value\"-data. That means that independent values are
 * stored by the nodes delegating the calls of the interface {@link ValueNode} to this class.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public class ValNodeDelegate extends AbstractForwardingNode implements ValueNode {

  /** Delegate for common node information. */
  private NodeDelegate mDelegate;

  /** Storing the value. */
  private byte[] mVal;

  /** Determines if input has been compressed. */
  private boolean mCompressed;

  /**
   * Constructor
   *
   * @param nodeDel {@link NodeDelegate} reference
   * @param val the value
   * @param compressed compress value or not
   */
  public ValNodeDelegate(final NodeDelegate nodeDel, final byte[] val, final boolean compressed) {
    assert nodeDel != null : "nodeDel must not be null!";
    assert val != null : "val must not be null!";
    mDelegate = nodeDel;
    mVal = val;
    mCompressed = compressed;
  }

  @Override
  public VisitResultType acceptVisitor(final Visitor visitor) {
    return mDelegate.acceptVisitor(visitor);
  }

  @Override
  public byte[] getRawValue() {
    return mCompressed
        ? Compression.decompress(mVal)
        : mVal;
  }

  @Override
  public String getValue() {
    return new String(getRawValue(), Constants.DEFAULT_ENCODING);
  }

  /**
   * Get value which might be compressed.
   *
   * @return {@code value} which might be compressed
   */
  public byte[] getCompressed() {
    return mVal;
  }

  @Override
  public void setValue(final byte[] value) {
    mCompressed = new String(value).length() > 10
        ? true
        : false;
    mVal = mCompressed
        ? Compression.compress(value, Deflater.DEFAULT_COMPRESSION)
        : value;
  }

  /**
   * Determine if input value has been compressed.
   *
   * @return {@code true}, if it has been compressed, {@code false} otherwise
   */
  public boolean isCompressed() {
    return mCompressed;
  }

  /**
   * Set compression.
   *
   * @param compressed determines if value is compressed or not
   */
  public void setCompressed(final boolean compressed) {
    mCompressed = compressed;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mDelegate, mVal);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (!(obj instanceof ValNodeDelegate))
      return false;

    final ValNodeDelegate other = (ValNodeDelegate) obj;
    return Objects.equal(mDelegate, other.mDelegate) && Arrays.equals(mVal, other.mVal);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("value", new String(mVal)).toString();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return mDelegate.isSameItem(other);
  }

  @Override
  public Kind getKind() {
    return Kind.UNKNOWN;
  }

  @Override
  protected NodeDelegate delegate() {
    return mDelegate;
  }
}
