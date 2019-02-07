package org.sirix.api.json;

import org.sirix.api.NodeTrx;

public interface JsonNodeTrx extends JsonNodeReadOnlyTrx, NodeTrx {
  JsonNodeTrx insertObjectAsFirstChild();

  JsonNodeTrx insertObjectAsRightSibling();

  JsonNodeTrx insertObjectKeyAsFirstChild(String name);

  JsonNodeTrx insertObjectKeyAsRightSibling(String name);

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
}
