package io.sirix.access.trx.node.json;

import io.sirix.api.json.JsonNodeTrx;
import io.sirix.access.trx.node.InternalNodeTrx;

public interface InternalJsonNodeTrx extends InternalNodeTrx<JsonNodeTrx>, JsonNodeTrx {

  JsonNodeTrx insertStringValueAsFirstChild(byte[] utf8Value);

  JsonNodeTrx insertStringValueAsLastChild(byte[] utf8Value);

  JsonNodeTrx insertStringValueAsLeftSibling(byte[] utf8Value);

  JsonNodeTrx insertStringValueAsRightSibling(byte[] utf8Value);
}
