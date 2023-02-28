package org.sirix.service.json.shredder;

import com.google.gson.stream.JsonReader;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongStack;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Numeric;
import org.brackit.xquery.jdm.Item;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.jdm.json.Array;
import org.brackit.xquery.jdm.json.Object;
import org.sirix.access.trx.node.json.objectvalue.*;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.service.InsertPosition;
import org.sirix.service.ShredderCommit;
import org.sirix.service.json.JsonNumber;
import org.sirix.settings.Fixed;

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
  private final LongStack parents;

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

    parents = new LongArrayList();
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
    } else if (s instanceof Array a) {
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
        parents.popLong();
        wtx.moveTo(parents.peekLong(0));

        if (parent instanceof Object) {
          parents.popLong();
          wtx.moveTo(parents.peekLong(0));
        }
      }
    } else if (s instanceof Object object) {
      level++;
      if (!(level == 1 && skipRootJson)) {
        if (objectField != null) {
          final var value = new ObjectValue();
          addObjectRecord(objectField, value, nextTokenIsParent);
        } else {
          addObject();
        }
      }

      for (int i = 0; i < object.len(); i++) {
        final var value = object.value(i);
        json(object, value, object.name(i).stringValue(),
            i + 1 == object.len() || !(value instanceof Array) && !(value instanceof Object));
      }

      level--;

      if (!(level == 0 && skipRootJson)) {
        parents.popLong();
        wtx.moveTo(parents.peekLong(0));

        if (parent instanceof Object) {
          parents.popLong();
          wtx.moveTo(parents.peekLong(0));
        }
      }
    }
  }

  /**
   * Insert new content based on a StAX like parser.
   *
   * @throws SirixException if something went wrong while inserting
   */
  private void insertNewContent() {
    level = 0;

    json(null, item, null, false);
  }

  private long insertStringValue(final String stringValue, final boolean nextTokenIsParent) {
    final String value = checkNotNull(stringValue);
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertStringValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertStringValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertStringValueAsLeftSibling(value).getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
      default -> throw new AssertionError();//Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertBooleanValue(final boolean boolValue, final boolean nextTokenIsParent) {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertBooleanValueAsFirstChild(boolValue).getNodeKey();
        } else {
          key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertBooleanValueAsLastChild(boolValue).getNodeKey();
        } else {
          key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertBooleanValueAsLeftSibling(boolValue).getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
      default -> throw new AssertionError();//Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertNumberValue(final Number numberValue, final boolean nextTokenIsParent) {
    final Number value = checkNotNull(numberValue);

    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNumberValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNumberValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertNumberValueAsLeftSibling(value).getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
      default -> throw new AssertionError();//Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private void adaptTrxPosAndStack(final boolean nextTokenIsParent, final long key) {
    parents.popLong();

    if (nextTokenIsParent)
      wtx.moveTo(parents.peekLong(0));
    else
      parents.push(key);
  }

  private long insertNullValue(final boolean nextTokenIsParent) {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNullValueAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertNullValueAsRightSibling().getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNullValueAsLastChild().getNodeKey();
        } else {
          key = wtx.insertNullValueAsRightSibling().getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertNullValueAsLeftSibling().getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertNullValueAsRightSibling().getNodeKey();
      default -> throw new AssertionError();//Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertArray() {
    long key = -1;
    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsLastChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> {
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertArrayAsLeftSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
      }
      case AS_RIGHT_SIBLING -> {
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertArrayAsRightSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
      }
      // $CASES-OMITTED$
      default -> throw new AssertionError();// Must not happen.
    }

    parents.popLong();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private long addObject() {
    long key = -1;
    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsLastChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> {
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertObjectAsLeftSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
      }
      case AS_RIGHT_SIBLING -> {
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertObjectAsRightSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
      }
      // $CASES-OMITTED$
      default -> throw new AssertionError();// Must not happen.
    }

    parents.popLong();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private void addObjectRecord(final String name, final ObjectRecordValue<?> value,
      final boolean isNextTokenParentToken) {
    assert name != null;

    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectRecordAsFirstChild(name, value).getNodeKey();
        } else {
          key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectRecordAsLastChild(name, value).getNodeKey();
        } else {
          key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertObjectRecordAsLeftSibling(name, value).getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
      default -> throw new AssertionError();//Should not happen
    }

    parents.popLong();
    parents.push(wtx.getParentKey());
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    if (wtx.getKind() == NodeKind.OBJECT || wtx.getKind() == NodeKind.ARRAY) {
      parents.popLong();
      parents.push(key);
      parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
    } else {
      adaptTrxPosAndStack(isNextTokenParentToken, key);
    }
  }
}