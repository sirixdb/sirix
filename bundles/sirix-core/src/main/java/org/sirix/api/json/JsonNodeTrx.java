package org.sirix.api.json;

import org.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import org.sirix.api.NodeTrx;
import com.google.gson.stream.JsonReader;

public interface JsonNodeTrx extends JsonNodeReadOnlyTrx, NodeTrx {
  enum Commit {
    Implicit,

    No
  }

  enum CheckParentNode {
    Yes,

    No
  }

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

  JsonNodeTrx replaceObjectRecordValue(String key, ObjectRecordValue<?> value);

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

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader);

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader);

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsLeftSibling(JsonReader reader, Commit doImplicitCommit, CheckParentNode checkParentNode);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit doImplicitCommit);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, Commit doImplicitCommit, CheckParentNode checkParentNode);
}
