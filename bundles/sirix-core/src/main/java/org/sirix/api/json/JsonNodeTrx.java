package org.sirix.api.json;

import org.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import org.sirix.api.NodeTrx;
import com.google.gson.stream.JsonReader;

public interface JsonNodeTrx extends JsonNodeReadOnlyTrx, NodeTrx {
  JsonNodeTrx insertObjectAsFirstChild();

  JsonNodeTrx insertObjectAsLastChild();

  JsonNodeTrx insertObjectAsRightSibling();

  JsonNodeTrx insertObjectRecordAsFirstChild(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertObjectRecordAsLastChild(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertObjectRecordAsRightSibling(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertArrayAsFirstChild();

  JsonNodeTrx insertArrayAsLastChild();

  JsonNodeTrx insertArrayAsRightSibling();

  JsonNodeTrx replaceObjectRecordValue(String key, ObjectRecordValue<?> value);

  JsonNodeTrx setObjectKeyName(String key);

  JsonNodeTrx setStringValue(String value);

  JsonNodeTrx setBooleanValue(boolean value);

  JsonNodeTrx setNumberValue(Number value);

  JsonNodeTrx remove();

  JsonNodeTrx insertStringValueAsFirstChild(String value);

  JsonNodeTrx insertStringValueAsLastChild(String value);

  JsonNodeTrx insertStringValueAsRightSibling(String value);

  JsonNodeTrx insertBooleanValueAsFirstChild(boolean value);

  JsonNodeTrx insertBooleanValueAsLastChild(boolean value);

  JsonNodeTrx insertBooleanValueAsRightSibling(boolean value);

  JsonNodeTrx insertNumberValueAsFirstChild(Number value);

  JsonNodeTrx insertNumberValueAsLastChild(Number value);

  JsonNodeTrx insertNumberValueAsRightSibling(Number value);

  JsonNodeTrx insertNullValueAsFirstChild();

  JsonNodeTrx insertNullValueAsLastChild();

  JsonNodeTrx insertNullValueAsRightSibling();

  JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader);

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader);

  JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader, boolean doImplicitCommit);

  JsonNodeTrx insertSubtreeAsLastChild(JsonReader reader, boolean doImplicitCommit);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader, boolean doImplicitCommit);
}
