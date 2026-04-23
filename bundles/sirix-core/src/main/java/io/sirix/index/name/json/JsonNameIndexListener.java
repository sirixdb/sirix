package io.sirix.index.name.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import org.jspecify.annotations.Nullable;
import io.sirix.index.name.NameIndexListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;

final class JsonNameIndexListener implements PathNodeKeyChangeListener {

  private final NameIndexListener indexListener;

  public JsonNameIndexListener(final NameIndexListener listener) {
    indexListener = listener;
  }

  @Override
  public void listen(IndexController.ChangeType type, ImmutableNode node, long pathNodeKey) {
    if (node instanceof final ObjectKeyNode objectKeyNode) {
      listen(type, objectKeyNode.getNodeKey(), objectKeyNode.getKind(), pathNodeKey, objectKeyNode.getName(), null);
    } else if (node instanceof final ObjectNamedNumberNode n) {
      // Fused OBJECT_NAMED_* — same name-index entry as the OBJECT_KEY role it replaces.
      // getName() on fused nodes reads the cached QNm; if null, the upstream notify path
      // resolves it via the storage-engine name index before dispatch, so we trust what
      // we got. If name is null here, we skip (matches the OBJECT_KEY path's null guard).
      listen(type, n.getNodeKey(), n.getKind(), pathNodeKey, n.getName(), null);
    } else if (node instanceof final ObjectNamedStringNode n) {
      listen(type, n.getNodeKey(), n.getKind(), pathNodeKey, n.getName(), null);
    } else if (node instanceof final ObjectNamedBooleanNode n) {
      listen(type, n.getNodeKey(), n.getKind(), pathNodeKey, n.getName(), null);
    } else if (node instanceof final ObjectNamedNullNode n) {
      listen(type, n.getNodeKey(), n.getKind(), pathNodeKey, n.getName(), null);
    }
  }

  @Override
  public void listen(IndexController.ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey,
      @Nullable QNm name, @Nullable Str value) {
    if ((nodeKind == NodeKind.OBJECT_KEY
            || nodeKind == NodeKind.OBJECT_NAMED_BOOLEAN || nodeKind == NodeKind.OBJECT_NAMED_NUMBER
            || nodeKind == NodeKind.OBJECT_NAMED_STRING || nodeKind == NodeKind.OBJECT_NAMED_NULL)
        && name != null) {
      indexListener.listen(type, nodeKey, name);
    }
  }
}
