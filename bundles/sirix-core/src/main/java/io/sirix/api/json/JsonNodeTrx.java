package io.sirix.api.json;

import com.fasterxml.jackson.core.JsonParser;
import com.google.gson.stream.JsonReader;
import io.brackit.query.jdm.Item;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.api.NodeTrx;
import io.sirix.exception.SirixException;

public interface JsonNodeTrx extends JsonNodeReadOnlyTrx, NodeTrx {
  enum Commit {
    IMPLICIT,

    NO
  }

  enum CheckParentNode {
    YES,

    NO
  }

  enum SkipRootToken {
    YES,

    NO
  }

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the provided
   * transaction) and insert as right sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link JsonNodeReadOnlyTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  JsonNodeTrx copySubtreeAsFirstChild(JsonNodeReadOnlyTrx rtx);

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the provided
   * transaction) and insert as left sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link JsonNodeReadOnlyTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  JsonNodeTrx copySubtreeAsLeftSibling(JsonNodeReadOnlyTrx rtx);

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the provided
   * transaction) and insert as right sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link JsonNodeReadOnlyTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  JsonNodeTrx copySubtreeAsRightSibling(JsonNodeReadOnlyTrx rtx);

  JsonNodeTrx insertObjectAsFirstChild();

  JsonNodeTrx insertObjectAsLastChild();

  JsonNodeTrx insertObjectAsLeftSibling();

  JsonNodeTrx insertObjectAsRightSibling();

  JsonNodeTrx insertObjectRecordAsFirstChild(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertObjectRecordAsLastChild(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertObjectRecordAsLeftSibling(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertObjectRecordAsRightSibling(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertArrayAsFirstChild();

  JsonNodeTrx insertArrayAsLastChild();

  JsonNodeTrx insertArrayAsLeftSibling();

  JsonNodeTrx insertArrayAsRightSibling();

  JsonNodeTrx replaceObjectRecordValue(ObjectRecordValue<?> value);

  JsonNodeTrx setObjectKeyName(String key);

  JsonNodeTrx setStringValue(String value);

  JsonNodeTrx setBooleanValue(boolean value);

  JsonNodeTrx setNumberValue(Number value);

  JsonNodeTrx remove();

  JsonNodeTrx insertStringValueAsFirstChild(String value);

  JsonNodeTrx insertStringValueAsLastChild(String value);

  JsonNodeTrx insertStringValueAsLeftSibling(String value);

  JsonNodeTrx insertStringValueAsRightSibling(String value);

  JsonNodeTrx insertBooleanValueAsFirstChild(boolean value);

  JsonNodeTrx insertBooleanValueAsLastChild(boolean value);

  JsonNodeTrx insertBooleanValueAsLeftSibling(boolean value);

  JsonNodeTrx insertBooleanValueAsRightSibling(boolean value);

  JsonNodeTrx insertNumberValueAsFirstChild(Number value);

  JsonNodeTrx insertNumberValueAsLastChild(Number value);

  JsonNodeTrx insertNumberValueAsLeftSibling(Number value);

  JsonNodeTrx insertNumberValueAsRightSibling(Number value);

  JsonNodeTrx insertNullValueAsFirstChild();

  JsonNodeTrx insertNullValueAsLastChild();

  JsonNodeTrx insertNullValueAsLeftSibling();

  JsonNodeTrx insertNullValueAsRightSibling();

  // ==================== JsonReader Subtree Methods ====================

  JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader) {
    return insertSubtreeAsFirstChild(reader, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit commit) {
    return insertSubtreeAsFirstChild(reader, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsFirstChild(reader, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader) {
    return insertSubtreeAsLastChild(reader, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit commit) {
    return insertSubtreeAsLastChild(reader, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsLastChild(reader, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader) {
    return insertSubtreeAsLeftSibling(reader, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit commit) {
    return insertSubtreeAsLeftSibling(reader, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsLeftSibling(reader, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader) {
    return insertSubtreeAsRightSibling(reader, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit commit) {
    return insertSubtreeAsRightSibling(reader, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsRightSibling(reader, commit, checkParentNode, SkipRootToken.NO);
  }

  // ==================== Item Subtree Methods ====================

  JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsFirstChild(Item item) {
    return insertSubtreeAsFirstChild(item, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit commit) {
    return insertSubtreeAsFirstChild(item, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsFirstChild(item, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsLastChild(Item item) {
    return insertSubtreeAsLastChild(item, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit commit) {
    return insertSubtreeAsLastChild(item, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsLastChild(item, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsLeftSibling(Item item) {
    return insertSubtreeAsLeftSibling(item, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit commit) {
    return insertSubtreeAsLeftSibling(item, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsLeftSibling(item, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsRightSibling(Item item) {
    return insertSubtreeAsRightSibling(item, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit commit) {
    return insertSubtreeAsRightSibling(item, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsRightSibling(item, commit, checkParentNode, SkipRootToken.NO);
  }

  // ==================== Jackson JsonParser Methods ====================

  JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser) {
    return insertSubtreeAsFirstChild(parser, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit commit) {
    return insertSubtreeAsFirstChild(parser, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsFirstChild(parser, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser) {
    return insertSubtreeAsLastChild(parser, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit commit) {
    return insertSubtreeAsLastChild(parser, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsLastChild(parser, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser) {
    return insertSubtreeAsLeftSibling(parser, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit commit) {
    return insertSubtreeAsLeftSibling(parser, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsLeftSibling(parser, commit, checkParentNode, SkipRootToken.NO);
  }

  JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  default JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser) {
    return insertSubtreeAsRightSibling(parser, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit commit) {
    return insertSubtreeAsRightSibling(parser, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  default JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtreeAsRightSibling(parser, commit, checkParentNode, SkipRootToken.NO);
  }
}
