package io.sirix.index.cas.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
import io.sirix.node.NodeKind;
import io.brackit.query.atomic.Str;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.jdm.Type;
import io.sirix.index.AtomicUtil;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.index.cas.CASIndexListener;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.brackit.query.atomic.QNm;
import org.jspecify.annotations.Nullable;

public final class JsonCASIndexListener implements PathNodeKeyChangeListener {

  private static final Str STR_TRUE = new Str("true");
  private static final Str STR_FALSE = new Str("false");

  private final CASIndexListener indexListenerDelegate;

  public JsonCASIndexListener(final CASIndexListener indexListenerDelegate) {
    this.indexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    if (node.getKind() == NodeKind.NUMBER_VALUE || node.getKind() == NodeKind.OBJECT_NAMED_NUMBER) {
      final Number number = switch (node) {
        case NumberNode value -> value.getValue();
        case ImmutableNumberNode value -> value.getValue();
        case ObjectNamedNumberNode value -> value.getValue();
        default -> throw new IllegalArgumentException("Unsupported numeric node: " + node.getKind());
      };
      indexListenerDelegate.listen(type, node.getNodeKey(), pathNodeKey, AtomicUtil.fromNumber(number));
      return;
    }
    final Str value = extractValue(node);
    listen(type, node.getNodeKey(), node.getKind(), pathNodeKey, null, value);
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    switch (nodeKind) {
      case OBJECT_NAMED_ARRAY -> indexListenerDelegate.rejectValue(pathNodeKey, true);
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_NULL, NULL_VALUE -> indexListenerDelegate.rejectValue(pathNodeKey, false);
      default -> { }
    }
    if (value == null) {
      return;
    }
    switch (nodeKind) {
      case NUMBER_VALUE, OBJECT_NAMED_NUMBER -> {
        final Atomic number = indexListenerDelegate.getContentType().instanceOf(Type.INR)
            ? AtomicUtil.fromNumericString(value.stringValue())
            : value;
        indexListenerDelegate.listen(type, nodeKey, pathNodeKey, number);
      }
      case STRING_VALUE, BOOLEAN_VALUE,
          // Fused kinds carry primitive value inline — extractValue upstream produced the Str.
          OBJECT_NAMED_STRING, OBJECT_NAMED_BOOLEAN ->
        indexListenerDelegate.listen(type, nodeKey, pathNodeKey, value);
      default -> {
      }
    }
  }

  private static Str extractValue(final ImmutableNode node) {
    switch (node.getKind()) {
      case STRING_VALUE -> {
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
        return boolValue
            ? STR_TRUE
            : STR_FALSE;
      }
      // Fused OBJECT_NAMED_* records carry the primitive value inline. The production write path
      // passes the value precomputed to the two-arg listen overload, but THIS single-arg overload
      // accepts fused nodes too — without these cases it silently skipped indexing them.
      case OBJECT_NAMED_STRING -> {
        if (node instanceof ValueNode valueNode) {
          return new Str(valueNode.getValue());
        }
        throw new IllegalStateException("Unexpected node type for fused string value: " + node.getClass());
      }
      case OBJECT_NAMED_BOOLEAN -> {
        if (node instanceof ObjectNamedBooleanNode fused) {
          return fused.getValue() ? STR_TRUE : STR_FALSE;
        }
        throw new IllegalStateException("Unexpected node type for fused boolean value: " + node.getClass());
      }
      default -> {
        return null;
      }
    }
  }
}
