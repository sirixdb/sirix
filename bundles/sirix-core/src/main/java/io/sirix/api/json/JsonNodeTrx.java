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

  JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader);

  JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader);

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader);

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  JsonNodeTrx insertSubtreeAsFirstChild(Item item);

  JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsFirstChild(Item item, Commit doImplicitCommit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  JsonNodeTrx insertSubtreeAsLastChild(Item item);

  JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsLastChild(Item item, Commit doImplicitCommit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  JsonNodeTrx insertSubtreeAsLeftSibling(Item item);

  JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsLeftSibling(Item item, Commit doImplicitCommit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  JsonNodeTrx insertSubtreeAsRightSibling(Item item);

  JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsRightSibling(Item item, Commit doImplicitCommit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  // ==================== Jackson JsonParser Methods ====================

  /**
   * Insert a subtree as first child using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser);

  /**
   * Insert a subtree as first child using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param doImplicitCommit whether to commit implicitly after insertion
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit doImplicitCommit);

  /**
   * Insert a subtree as first child using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param doImplicitCommit whether to commit implicitly after insertion
   * @param checkParentNode whether to check parent node validity
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit doImplicitCommit, CheckParentNode checkParentNode);

  /**
   * Insert a subtree as first child using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param commit whether to commit implicitly after insertion
   * @param checkParentNode whether to check parent node validity
   * @param skipRootToken whether to skip the root token
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsFirstChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  /**
   * Insert a subtree as last child using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser);

  /**
   * Insert a subtree as last child using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param doImplicitCommit whether to commit implicitly after insertion
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit doImplicitCommit);

  /**
   * Insert a subtree as last child using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param doImplicitCommit whether to commit implicitly after insertion
   * @param checkParentNode whether to check parent node validity
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit doImplicitCommit, CheckParentNode checkParentNode);

  /**
   * Insert a subtree as last child using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param commit whether to commit implicitly after insertion
   * @param checkParentNode whether to check parent node validity
   * @param skipRootToken whether to skip the root token
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsLastChild(JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  /**
   * Insert a subtree as left sibling using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser);

  /**
   * Insert a subtree as left sibling using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param doImplicitCommit whether to commit implicitly after insertion
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit doImplicitCommit);

  /**
   * Insert a subtree as left sibling using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param doImplicitCommit whether to commit implicitly after insertion
   * @param checkParentNode whether to check parent node validity
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit doImplicitCommit, CheckParentNode checkParentNode);

  /**
   * Insert a subtree as left sibling using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param commit whether to commit implicitly after insertion
   * @param checkParentNode whether to check parent node validity
   * @param skipRootToken whether to skip the root token
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsLeftSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);

  /**
   * Insert a subtree as right sibling using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser);

  /**
   * Insert a subtree as right sibling using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param doImplicitCommit whether to commit implicitly after insertion
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit doImplicitCommit);

  /**
   * Insert a subtree as right sibling using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param doImplicitCommit whether to commit implicitly after insertion
   * @param checkParentNode whether to check parent node validity
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit doImplicitCommit, CheckParentNode checkParentNode);

  /**
   * Insert a subtree as right sibling using Jackson streaming parser.
   *
   * @param parser Jackson {@link JsonParser} for streaming JSON input
   * @param commit whether to commit implicitly after insertion
   * @param checkParentNode whether to check parent node validity
   * @param skipRootToken whether to skip the root token
   * @return the transaction instance
   */
  JsonNodeTrx insertSubtreeAsRightSibling(JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken);
}
