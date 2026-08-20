package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.InternalNodeReadOnlyTrx;
import io.sirix.access.trx.node.json.objectvalue.PrimitiveNumberValue;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;

public interface InternalJsonNodeReadOnlyTrx
    extends InternalNodeReadOnlyTrx<ImmutableNode>, JsonNodeReadOnlyTrx, PrimitiveNumberCursor, FusedStringCursor {

  @Override
  default byte readFusedPrimitiveNumber(final long[] valueOut, final int index) {
    if (getStructuralNodeView() instanceof ObjectNamedNumberNode numberNode) {
      return numberNode.readPrimitiveNumber(valueOut, index);
    }
    return PrimitiveNumberValue.NONE;
  }

  @Override
  default int readFusedStringUtf8(final byte[] valueOut) {
    if (getStructuralNodeView() instanceof ObjectNamedStringNode stringNode) {
      return stringNode.readFusedStringUtf8(valueOut);
    }
    return FusedStringCursor.UNAVAILABLE;
  }
}
