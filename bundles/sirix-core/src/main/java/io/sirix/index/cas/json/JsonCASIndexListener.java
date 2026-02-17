package io.sirix.index.cas.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
import io.sirix.node.NodeKind;
import io.brackit.query.atomic.Str;
import io.sirix.index.cas.CASIndexListener;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectNumberNode;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class JsonCASIndexListener implements PathNodeKeyChangeListener {

  private static final Str STR_TRUE = new Str("true");
  private static final Str STR_FALSE = new Str("false");

  private final CASIndexListener indexListenerDelegate;

  public JsonCASIndexListener(final CASIndexListener indexListenerDelegate) {
    this.indexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    final Str value = extractValue(node);
    listen(type, node.getNodeKey(), node.getKind(), pathNodeKey, null, value);
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    if (value == null) {
      return;
    }
    switch (nodeKind) {
      case STRING_VALUE, OBJECT_STRING_VALUE, BOOLEAN_VALUE, OBJECT_BOOLEAN_VALUE, NUMBER_VALUE, OBJECT_NUMBER_VALUE ->
          indexListenerDelegate.listen(type, nodeKey, pathNodeKey, value);
      default -> {
      }
    }
  }

  private static Str extractValue(final ImmutableNode node) {
    switch (node.getKind()) {
      case STRING_VALUE, OBJECT_STRING_VALUE -> {
        // Handle both mutable ValueNode and immutable ImmutableValueNode
        final String value;
        if (node instanceof ValueNode valueNode) {
          value = valueNode.getValue();
        } else if (node instanceof ImmutableValueNode immutableValueNode) {
          value = immutableValueNode.getValue();
        } else {
          throw new IllegalStateException("Unexpected node type for string value: " + node.getClass());
        }
        return new Str(value);
      }
      case BOOLEAN_VALUE -> {
        final boolean boolValue;
        if (node instanceof BooleanNode boolNode) {
          boolValue = boolNode.getValue();
        } else if (node instanceof ImmutableBooleanNode immutableBoolNode) {
          boolValue = immutableBoolNode.getValue();
        } else {
          throw new IllegalStateException("Unexpected node type for boolean value: " + node.getClass());
        }
        return boolValue ? STR_TRUE : STR_FALSE;
      }
      case OBJECT_BOOLEAN_VALUE -> {
        final boolean objBoolValue;
        if (node instanceof ObjectBooleanNode objBoolNode) {
          objBoolValue = objBoolNode.getValue();
        } else if (node instanceof ImmutableObjectBooleanNode immutableObjBoolNode) {
          objBoolValue = immutableObjBoolNode.getValue();
        } else {
          throw new IllegalStateException("Unexpected node type for object boolean value: " + node.getClass());
        }
        return objBoolValue ? STR_TRUE : STR_FALSE;
      }
      case NUMBER_VALUE -> {
        final Number numValue;
        if (node instanceof NumberNode numNode) {
          numValue = numNode.getValue();
        } else if (node instanceof ImmutableNumberNode immutableNumNode) {
          numValue = immutableNumNode.getValue();
        } else {
          throw new IllegalStateException("Unexpected node type for number value: " + node.getClass());
        }
        return new Str(String.valueOf(numValue));
      }
      case OBJECT_NUMBER_VALUE -> {
        final Number objNumValue;
        if (node instanceof ObjectNumberNode objNumNode) {
          objNumValue = objNumNode.getValue();
        } else if (node instanceof ImmutableObjectNumberNode immutableObjNumNode) {
          objNumValue = immutableObjNumNode.getValue();
        } else {
          throw new IllegalStateException("Unexpected node type for object number value: " + node.getClass());
        }
        return new Str(String.valueOf(objNumValue));
      }
      default -> {
        return null;
      }
    }
  }
}
