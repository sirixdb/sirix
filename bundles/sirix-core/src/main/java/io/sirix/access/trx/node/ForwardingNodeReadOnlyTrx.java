package io.sirix.access.trx.node;

import io.brackit.query.atomic.QNm;
import io.sirix.access.User;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.ResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;

import java.time.Instant;
import java.util.Optional;

/**
 * A forwarding {@link NodeReadOnlyTrx} based on the decorator pattern.
 *
 * @author Joao Sousa
 */
public interface ForwardingNodeReadOnlyTrx extends NodeReadOnlyTrx {

  NodeReadOnlyTrx delegate();

  @Override
  default int getId() {
    return delegate().getId();
  }

  @Override
  default int getRevisionNumber() {
    return delegate().getRevisionNumber();
  }

  @Override
  default Instant getRevisionTimestamp() {
    return delegate().getRevisionTimestamp();
  }

  @Override
  default long getMaxNodeKey() {
    return delegate().getMaxNodeKey();
  }

  @Override
  default void close() {
    delegate().close();
  }

  @Override
  default long getNodeKey() {
    return delegate().getNodeKey();
  }

  @Override
  default ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceSession() {
    return delegate().getResourceSession();
  }

  @Override
  default CommitCredentials getCommitCredentials() {
    return delegate().getCommitCredentials();
  }

  @Override
  default boolean moveTo(long key) {
    return delegate().moveTo(key);
  }

  @Override
  default StorageEngineReader getStorageEngineReader() {
    return delegate().getStorageEngineReader();
  }

  @Override
  default long getPathNodeKey() {
    return delegate().getPathNodeKey();
  }

  @Override
  default int keyForName(String name) {
    return delegate().keyForName(name);
  }

  @Override
  default String nameForKey(int key) {
    return delegate().nameForKey(key);
  }

  @Override
  default long getDescendantCount() {
    return delegate().getDescendantCount();
  }

  @Override
  default long getChildCount() {
    return delegate().getChildCount();
  }

  @Override
  default NodeKind getPathKind() {
    return delegate().getPathKind();
  }

  @Override
  default boolean isDocumentRoot() {
    return delegate().isDocumentRoot();
  }

  @Override
  default boolean isClosed() {
    return delegate().isClosed();
  }

  @Override
  default QNm getName() {
    return delegate().getName();
  }

  @Override
  default boolean hasChildren() {
    return delegate().hasChildren();
  }

  @Override
  default long getHash() {
    return delegate().getHash();
  }

  @Override
  default String getValue() {
    return delegate().getValue();
  }

  @Override
  default Optional<User> getUser() {
    return delegate().getUser();
  }

  @Override
  default boolean storeDeweyIDs() {
    return delegate().storeDeweyIDs();
  }

  @Override
  default SirixDeweyID getDeweyID() {
    return delegate().getDeweyID();
  }
}
