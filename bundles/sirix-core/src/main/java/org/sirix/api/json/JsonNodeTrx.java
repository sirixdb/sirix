package org.sirix.api.json;

import org.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import org.sirix.api.NodeTrx;
import com.google.gson.stream.JsonReader;

public interface JsonNodeTrx extends JsonNodeReadOnlyTrx, NodeTrx {
  JsonNodeTrx insertObjectAsFirstChild();

  JsonNodeTrx insertObjectAsRightSibling();

  JsonNodeTrx insertObjectRecordAsFirstChild(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertObjectRecordAsRightSibling(String key, ObjectRecordValue<?> value);

  JsonNodeTrx insertArrayAsFirstChild();

  JsonNodeTrx insertArrayAsRightSibling();

  JsonNodeTrx setObjectKeyName(String name);

  JsonNodeTrx setStringValue(String value);

  JsonNodeTrx setBooleanValue(boolean value);

  JsonNodeTrx setNumberValue(Number value);

  JsonNodeTrx remove();

  JsonNodeTrx insertStringValueAsFirstChild(String value);

  JsonNodeTrx insertStringValueAsRightSibling(String value);

  JsonNodeTrx insertBooleanValueAsFirstChild(boolean value);

  JsonNodeTrx insertBooleanValueAsRightSibling(boolean value);

  JsonNodeTrx insertNumberValueAsFirstChild(Number value);

  JsonNodeTrx insertNumberValueAsRightSibling(Number value);

  JsonNodeTrx insertNullValueAsFirstChild();

  JsonNodeTrx insertNullValueAsRightSibling();

  JsonNodeTrx insertSubtreeAsFirstChild(JsonReader reader);

  JsonNodeTrx insertSubtreeAsRightSibling(JsonReader reader);
}
