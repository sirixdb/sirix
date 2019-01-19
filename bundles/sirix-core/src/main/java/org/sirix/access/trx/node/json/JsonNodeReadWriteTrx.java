package org.sirix.access.trx.node.json;

import org.sirix.api.NodeWriteTrx;

public interface JsonNodeReadWriteTrx extends JsonNodeReadOnlyTrx, NodeWriteTrx {
  JsonNodeReadWriteTrx insertObjectAsFirstChild();

  JsonNodeReadWriteTrx insertObjectAsRightSibling();

  JsonNodeReadWriteTrx insertObjectKeyAsFirstChild(String name);

  JsonNodeReadWriteTrx insertObjectKeyAsRightSibling(String name);

  JsonNodeReadWriteTrx insertArrayAsFirstChild();

  JsonNodeReadWriteTrx insertArrayAsRightSibling();

  JsonNodeReadWriteTrx setObjectRecordName(String name);

  JsonNodeReadWriteTrx setStringValue(String value);

  JsonNodeReadWriteTrx setBooleanValue(boolean value);

  JsonNodeReadWriteTrx setNumberValue(double value);

  JsonNodeReadWriteTrx remove();

  JsonNodeReadWriteTrx insertStringValueAsFirstChild(String value);

  JsonNodeReadWriteTrx insertStringValueAsRightSibling(String value);

  JsonNodeReadWriteTrx insertBooleanValueAsFirstChild(boolean value);

  JsonNodeReadWriteTrx insertBooleanValueAsRightSibling(boolean value);

  JsonNodeReadWriteTrx insertNumberValueAsFirstChild(double value);

  JsonNodeReadWriteTrx insertNumberValueAsRightSibling(double value);

  JsonNodeReadWriteTrx insertNullValueAsFirstChild();

  JsonNodeReadWriteTrx insertNullValueAsRightSibling();
}
