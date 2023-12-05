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

package io.sirix.service.json.shredder;

import io.sirix.access.trx.node.json.objectvalue.*;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.ShredderCommit;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongStack;

import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Serializes a subtree into the JSON-format.
 * </p>
 */
public final class JsonResourceCopy implements Callable<Void> {

  /**
   * {@link JsonNodeTrx}.
   */
  private final JsonNodeTrx wtx;

  /**
   * Determines if changes are going to be commit right after shredding.
   */
  private final ShredderCommit commit;

  private final JsonNodeReadOnlyTrx rtx;

  /** Keeps track of visited keys. */
  private final LongStack parents = new LongArrayList();

  private final long startNodeKey;

  /**
   * Insertion position.
   */
  private final InsertPosition insert;

  /**
   * Builder to build an {@link JsonItemShredder} instance.
   */
  public static class Builder {

    /**
     * {@link JsonNodeTrx} implementation.
     */
    private final JsonNodeTrx wtx;

    /**
     * The transaction to read from.
     */
    private final JsonNodeReadOnlyTrx rtx;

    /**
     * Insertion position.
     */
    private final InsertPosition insert;

    /**
     * Determines if after shredding the transaction should be immediately commited.
     */
    private ShredderCommit commit = ShredderCommit.NOCOMMIT;

    /**
     * Constructor.
     *
     * @param wtx    the transaction to write to
     * @param rtx    the transaction to read from
     * @param insert insertion position
     * @throws NullPointerException if one of the arguments is {@code null}
     */
    public Builder(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx, final InsertPosition insert) {
      this.wtx = requireNonNull(wtx);
      this.rtx = requireNonNull(rtx);
      this.insert = requireNonNull(insert);
    }

    /**
     * Commit afterwards.
     *
     * @return this builder instance
     */
    public JsonResourceCopy.Builder commitAfterwards() {
      commit = ShredderCommit.COMMIT;
      return this;
    }

    /**
     * Build an instance.
     *
     * @return {@link JsonItemShredder} instance
     */
    public JsonResourceCopy build() {
      return new JsonResourceCopy(wtx, rtx, this);
    }
  }

  /**
   * Stack for reading end element.
   */
  private final LongArrayList stack = new LongArrayList();

  /**
   * Private constructor.
   *
   * @param wtx     the transaction used to write
   * @param rtx     the trsnscation used to read
   * @param builder builder of the JSON resource copy
   */
  private JsonResourceCopy(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx, final Builder builder) {
    this.wtx = wtx;
    this.rtx = rtx;
    this.insert = builder.insert;
    this.commit = builder.commit;
    this.startNodeKey = rtx.getNodeKey();

    if (insert == InsertPosition.AS_FIRST_CHILD) {
      parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
    }
  }

  public Void call() {
    rtx.moveTo(startNodeKey);

    // Setup primitives.
    boolean moveToParent = false;
    boolean first = true;
    long key;
    long previousKey = Fixed.NULL_NODE_KEY.getStandardProperty();

    // Iterate over all nodes of the subtree including self.
    for (final var axis = new DescendantAxis(rtx, IncludeSelf.YES); axis.hasNext(); ) {
      key = axis.nextLong();
      // Process all pending moves to parents.
      if (moveToParent) {
        while (!stack.isEmpty() && stack.peekLong(0) != rtx.getLeftSiblingKey()) {
          rtx.moveTo(stack.popLong());
          rtx.moveTo(key);
          wtx.moveToParent();
        }
        if (!stack.isEmpty()) {
          rtx.moveTo(stack.popLong());
          wtx.moveToParent();
        }
        rtx.moveTo(key);
      }

      // Process node.
      final long nodeKey = rtx.getNodeKey();

      InsertPosition insertPosition;

      if (first) {
        insertPosition = insert;
      } else {
        if (moveToParent) {
          insertPosition = InsertPosition.AS_RIGHT_SIBLING;
        } else if (rtx.hasLeftSibling() && previousKey == rtx.getLeftSiblingKey()) {
          insertPosition = InsertPosition.AS_RIGHT_SIBLING;
        } else {
          insertPosition = InsertPosition.AS_FIRST_CHILD;
        }
      }

      moveToParent = false;
      // Values of object keys have already been inserted.
      if (first || rtx.getParentKind() != NodeKind.OBJECT_KEY) {
        processNode(rtx, insertPosition);
      }
      rtx.moveTo(nodeKey);

      first = false;

      // Push end element to stack if we are a start element with children.
      boolean withChildren = false;
      if (!rtx.isDocumentRoot() && rtx.hasFirstChild()) {
        stack.push(rtx.getNodeKey());
        withChildren = true;
      }

      // Remember to process all pending moves to parents from stack if required.
      if (!withChildren && !rtx.isDocumentRoot() && !rtx.hasRightSibling()) {
        moveToParent = true;
      }

      previousKey = key;
    }

    // Finally emit all pending moves to parents.
    while (!stack.isEmpty() && stack.peekLong(0) != Constants.NULL_ID_LONG) {
      rtx.moveTo(stack.popLong());
    }

    commit.commit(wtx);

    return null;
  }

  /**
   * Emit node.
   *
   * @param rtx Sirix {@link JsonNodeReadOnlyTrx}
   */
  public void processNode(final JsonNodeReadOnlyTrx rtx, final InsertPosition insertPosition) {
    switch (rtx.getKind()) {
      case JSON_DOCUMENT:
        break;
      case OBJECT:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertObjectAsFirstChild();
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertObjectAsRightSibling();
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case ARRAY:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertArrayAsFirstChild();
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertArrayAsRightSibling();
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case OBJECT_KEY:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          final var key = rtx.getName().getLocalName();
          rtx.moveToFirstChild();
          switch (rtx.getKind()) {
            case OBJECT -> wtx.insertObjectRecordAsFirstChild(key, new ObjectValue());
            case ARRAY -> wtx.insertObjectRecordAsFirstChild(key, new ArrayValue());
            case OBJECT_BOOLEAN_VALUE ->
                wtx.insertObjectRecordAsFirstChild(key, new BooleanValue(rtx.getBooleanValue()));
            case OBJECT_NULL_VALUE -> wtx.insertObjectRecordAsFirstChild(key, new NullValue());
            case OBJECT_STRING_VALUE -> wtx.insertObjectRecordAsFirstChild(key,
                                                                           new io.sirix.access.trx.node.json.objectvalue.StringValue(
                                                                               rtx.getValue()));
            case OBJECT_NUMBER_VALUE -> wtx.insertObjectRecordAsFirstChild(key, new NumberValue(rtx.getNumberValue()));
          }
          rtx.moveToParent();
        } else {
          final var key = rtx.getName().getLocalName();
          rtx.moveToFirstChild();
          switch (rtx.getKind()) {
            case OBJECT -> wtx.insertObjectRecordAsRightSibling(key, new ObjectValue());
            case ARRAY -> wtx.insertObjectRecordAsRightSibling(key, new ArrayValue());
            case OBJECT_BOOLEAN_VALUE ->
                wtx.insertObjectRecordAsRightSibling(key, new BooleanValue(rtx.getBooleanValue()));
            case OBJECT_NULL_VALUE -> wtx.insertObjectRecordAsRightSibling(key, new NullValue());
            case OBJECT_STRING_VALUE -> wtx.insertObjectRecordAsRightSibling(key,
                                                                           new io.sirix.access.trx.node.json.objectvalue.StringValue(
                                                                               rtx.getValue()));
            case OBJECT_NUMBER_VALUE -> wtx.insertObjectRecordAsRightSibling(key, new NumberValue(rtx.getNumberValue()));
          }
          rtx.moveToParent();
        }
        break;
      case BOOLEAN_VALUE:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertBooleanValueAsFirstChild(rtx.getBooleanValue());
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertBooleanValueAsRightSibling(rtx.getBooleanValue());
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case NULL_VALUE:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertNullValueAsFirstChild();
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertNullValueAsRightSibling();
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case NUMBER_VALUE:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertNumberValueAsFirstChild(rtx.getNumberValue());
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertNumberValueAsRightSibling(rtx.getNumberValue());
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case STRING_VALUE:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertStringValueAsFirstChild(rtx.getValue());
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertStringValueAsRightSibling(rtx.getValue());
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("Node kind not known!");
    }
  }
}
