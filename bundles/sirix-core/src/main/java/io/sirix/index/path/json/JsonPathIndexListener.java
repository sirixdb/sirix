package io.sirix.index.path.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.index.path.PathIndexListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.jspecify.annotations.Nullable;

final class JsonPathIndexListener implements PathNodeKeyChangeListener {

  private final PathIndexListener pathIndexListener;

  JsonPathIndexListener(final PathIndexListener pathIndexListenerDelegate) {
    pathIndexListener = pathIndexListenerDelegate;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    listen(type, node.getNodeKey(), node.getKind(), pathNodeKey, null, null);
  }

  @Override
  public void listen(IndexController.ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey,
      @Nullable QNm name, @Nullable Str value) {
    // Fused OBJECT_NAMED_* records play the OBJECT_KEY structural role — same PATH-index entry.
    // OBJECT_NAMED_ARRAY also plays the ARRAY structural role for path-index purposes.
    if (nodeKind == NodeKind.ARRAY || nodeKind.playsObjectKeyRole()) {
      pathIndexListener.listen(type, nodeKey, pathNodeKey);
    }
    // iter#32 P2 structural fusion mirror: OBJECT_NAMED_ARRAY's pathNodeKey points at the
    // {@code __array__/ARRAY} layer (so child fields nest correctly). Path-index lookups for
    // the OBJECT_KEY-level path "/.../tada" expect the OBJECT_KEY layer to also resolve, so
    // mirror the entry under the parent (OBJECT_KEY) PCR. Skip if the path-summary lookup
    // returns nothing (transient state during teardown).
    if (nodeKind == NodeKind.OBJECT_NAMED_ARRAY) {
      final var arrayPathNode = pathIndexListener.getPathSummaryReader().getPathNodeForPathNodeKey(pathNodeKey);
      if (arrayPathNode != null) {
        final long objectKeyLayerPathNodeKey = arrayPathNode.getParentKey();
        if (objectKeyLayerPathNodeKey >= 0) {
          pathIndexListener.listen(type, nodeKey, objectKeyLayerPathNodeKey);
        }
      }
    }
  }
}
