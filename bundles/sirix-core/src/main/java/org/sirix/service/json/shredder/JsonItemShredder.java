package org.sirix.service.json.shredder;

import com.google.gson.stream.JsonReader;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Numeric;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Type;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.Record;
import org.sirix.access.trx.node.json.objectvalue.*;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.service.ShredderCommit;
import org.sirix.service.json.JsonNumber;
import org.sirix.service.InsertPosition;
import org.sirix.settings.Fixed;

import javax.xml.stream.XMLStreamReader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class appends a given {@link JsonReader} to a {@link JsonNodeTrx} . The content of the
 * stream is added as a subtree. Based on an enum which identifies the point of insertion, the
 * subtree is either added as first child or as right sibling.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class JsonItemShredder implements Callable<Long> {

  /**
   * {@link JsonNodeTrx}.
   */
  private final JsonNodeTrx wtx;

  /**
   * {@link Item} implementation.
   */
  private final Item item;

  /**
   * Determines if changes are going to be commit right after shredding.
   */
  private final ShredderCommit commit;

  /**
   * Keeps track of visited keys.
   */
  private final Deque<Long> parents;

  /**
   * Insertion position.
   */
  private InsertPosition insert;

  private int level;

  private final boolean skipRootJson;

  /**
   * Builder to build an {@link JsonItemShredder} instance.
   */
  public static class Builder {

    /**
     * {@link JsonNodeTrx} implementation.
     */
    private final JsonNodeTrx wtx;

    /**
     * {@link Item} implementation.
     */
    private final Item item;

    /**
     * Insertion position.
     */
    private final InsertPosition insert;

    /**
     * Determines if after shredding the transaction should be immediately commited.
     */
    private ShredderCommit commit = ShredderCommit.NOCOMMIT;

    private boolean skipRootJsonToken;

    /**
     * Constructor.
     *
     * @param wtx    {@link JsonNodeTrx} implementation
     * @param item   {@link Item} implementation
     * @param insert insertion position
     * @throws NullPointerException if one of the arguments is {@code null}
     */
    public Builder(final JsonNodeTrx wtx, final Item item, final InsertPosition insert) {
      this.wtx = checkNotNull(wtx);
      this.item = checkNotNull(item);
      this.insert = checkNotNull(insert);
    }

    /**
     * Commit afterwards.
     *
     * @return this builder instance
     */
    public Builder commitAfterwards() {
      commit = ShredderCommit.COMMIT;
      return this;
    }

    public Builder skipRootJsonToken() {
      skipRootJsonToken = true;
      return this;
    }

    /**
     * Build an instance.
     *
     * @return {@link JsonItemShredder} instance
     */
    public JsonItemShredder build() {
      return new JsonItemShredder(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder builder reference
   */
  private JsonItemShredder(final Builder builder) {
    wtx = builder.wtx;
    item = builder.item;
    insert = builder.insert;
    commit = builder.commit;
    skipRootJson = builder.skipRootJsonToken;

    parents = new ArrayDeque<>();
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  /**
   * Invoking the shredder.
   *
   * @return revision of file
   * @throws SirixException if any kind of sirix exception which has occured
   */
  @Override
  public Long call() {
    final long revision = wtx.getRevisionNumber();
    insertNewContent();
    commit.commit(wtx);
    return revision;
  }

  private void json(Sequence parent, Sequence s, String objectField,
      boolean nextTokenIsParent) {
    if (s instanceof Atomic) {
      if (s instanceof Numeric) {
        final var number = JsonNumber.stringToNumber(s.toString());

        if (objectField != null) {
          final var value = new NumberValue(number);
          addObjectRecord(objectField, value, nextTokenIsParent);
        } else {
          insertNumberValue(number, nextTokenIsParent);
        }
      } else if (((Atomic) s).type() == Type.BOOL) {
        final var bool = s.booleanValue();

        if (objectField != null) {
          final var value = new BooleanValue(bool);
          addObjectRecord(objectField, value, nextTokenIsParent);
        } else {
          insertBooleanValue(bool, nextTokenIsParent);
        }
      } else if (((Atomic) s).type() == Type.NULL) {
        if (objectField != null) {
          final var value = new NullValue();
          addObjectRecord(objectField, value, nextTokenIsParent);
        } else {
          insertNullValue(nextTokenIsParent);
        }
      } else {
        final var str = ((Atomic) s).asStr().stringValue();

        if (objectField != null) {
          final var value = new StringValue(str);
          addObjectRecord(objectField, value, nextTokenIsParent);
        } else {
          insertStringValue(str, nextTokenIsParent);
        }
      }
    } else if (s instanceof Array) {
      Array a = (Array) s;

      level++;
      if (!(level == 1 && skipRootJson)) {
        if (objectField != null) {
          final var value = new ArrayValue();
          addObjectRecord(objectField, value, nextTokenIsParent);
        } else {
          insertArray();
        }
      }

      for (int i = 0; i < a.len(); i++) {
        final Sequence seq = a.at(i);
        json(a, seq, null, false);
      }

      level--;

      if (!(level == 0 && skipRootJson)) {
        parents.pop();
        wtx.moveTo(parents.peek());

        if (parent instanceof Record) {
          parents.pop();
          wtx.moveTo(parents.peek());
        }
      }
    } else if (s instanceof Record) {
      Record r = (Record) s;

      level++;
      if (!(level == 1 && skipRootJson)) {
        if (objectField != null) {
          final var value = new ObjectValue();
          addObjectRecord(objectField, value, nextTokenIsParent);
        } else {
          addObject();
        }
      }

      for (int i = 0; i < r.len(); i++) {
        final var value = r.value(i);
        json(r, value, r.name(i).stringValue(),
            i + 1 == r.len() || !(value instanceof Array) && !(value instanceof Record));
      }

      level--;

      if (!(level == 0 && skipRootJson)) {
        parents.pop();
        wtx.moveTo(parents.peek());

        if (parent instanceof Record) {
          parents.pop();
          wtx.moveTo(parents.peek());
        }
      }
    }
  }

  /**
   * Insert new content based on a StAX parser {@link XMLStreamReader}.
   *
   * @throws SirixException if something went wrong while inserting
   */
  protected final void insertNewContent() {
    level = 0;

    json(null, item, null, false);
  }

  private long insertStringValue(final String stringValue, final boolean nextTokenIsParent) {
    final String value = checkNotNull(stringValue);
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertStringValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertStringValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertStringValueAsLeftSibling(value).getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        break;
      default:
        throw new AssertionError();//Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertBooleanValue(final boolean boolValue, final boolean nextTokenIsParent) {
    final boolean value = checkNotNull(boolValue);
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertBooleanValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertBooleanValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertBooleanValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertBooleanValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertBooleanValueAsLeftSibling(value).getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertBooleanValueAsRightSibling(value).getNodeKey();
        break;
      default:
        throw new AssertionError();//Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertNumberValue(final Number numberValue, final boolean nextTokenIsParent) {
    final Number value = checkNotNull(numberValue);

    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNumberValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNumberValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertNumberValueAsLeftSibling(value).getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        break;
      default:
        throw new AssertionError();//Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private void adaptTrxPosAndStack(final boolean nextTokenIsParent, final long key) {
    parents.pop();

    if (nextTokenIsParent)
      wtx.moveTo(parents.peek());
    else
      parents.push(key);
  }

  private long insertNullValue(final boolean nextTokenIsParent) {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNullValueAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertNullValueAsRightSibling().getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNullValueAsLastChild().getNodeKey();
        } else {
          key = wtx.insertNullValueAsRightSibling().getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertNullValueAsLeftSibling().getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertNullValueAsRightSibling().getNodeKey();
        break;
      default:
        throw new AssertionError();//Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertArray() {
    long key = -1;
    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsLastChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertArrayAsLeftSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
        break;
      case AS_RIGHT_SIBLING:
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertArrayAsRightSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError();// Must not happen.
    }

    parents.pop();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private long addObject() {
    long key = -1;
    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsLastChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertObjectAsLeftSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
        break;
      case AS_RIGHT_SIBLING:
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertObjectAsRightSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError();// Must not happen.
    }

    parents.pop();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private void addObjectRecord(final String name, final ObjectRecordValue<?> value,
      final boolean isNextTokenParentToken) {
    assert name != null;

    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectRecordAsFirstChild(name, value).getNodeKey();
        } else {
          key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectRecordAsLastChild(name, value).getNodeKey();
        } else {
          key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertObjectRecordAsLeftSibling(name, value).getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        break;
      default:
        throw new AssertionError();//Should not happen
    }

    parents.pop();
    parents.push(wtx.getParentKey());
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    if (wtx.getKind() == NodeKind.OBJECT || wtx.getKind() == NodeKind.ARRAY) {
      parents.pop();
      parents.push(key);
      parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
    } else {
      adaptTrxPosAndStack(isNextTokenParentToken, key);
    }
  }
}