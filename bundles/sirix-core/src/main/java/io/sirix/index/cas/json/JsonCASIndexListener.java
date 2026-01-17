package io.sirix.index.cas.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.ChangeListener;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
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

public final class JsonCASIndexListener implements ChangeListener {

  private final CASIndexListener indexListenerDelegate;

  public JsonCASIndexListener(final CASIndexListener indexListenerDelegate) {
    this.indexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
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
        indexListenerDelegate.listen(type, node, pathNodeKey, new Str(value));
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
        indexListenerDelegate.listen(type, node, pathNodeKey, new Str(String.valueOf(boolValue)));
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
        indexListenerDelegate.listen(type, node, pathNodeKey, new Str(String.valueOf(objBoolValue)));
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
        indexListenerDelegate.listen(type, node, pathNodeKey, new Str(String.valueOf(numValue)));
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
        indexListenerDelegate.listen(type, node, pathNodeKey, new Str(String.valueOf(objNumValue)));
      }
    }
  }
}
