package org.sirix.access.trx.node;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.User;
import org.sirix.api.Move;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Optional;

/**
 * TODO: Class AbstractForwardingNodeReadOnlyTrx's description.
 *
 * @author Joao Sousa
 */
public interface ForwardingNodeReadOnlyTrx extends NodeReadOnlyTrx {
    
    NodeReadOnlyTrx delegate();

    @Override
    default long getId() {
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
    default ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceManager() {
        return delegate().getResourceManager();
    }

    @Override
    default CommitCredentials getCommitCredentials() {
        return delegate().getCommitCredentials();
    }

    @Override
    default Move<? extends NodeCursor> moveTo(long key) {
        return delegate().moveTo(key);
    }

    @Override
    default PageReadOnlyTrx getPageTrx() {
        return delegate().getPageTrx();
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
    default BigInteger getHash() {
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
