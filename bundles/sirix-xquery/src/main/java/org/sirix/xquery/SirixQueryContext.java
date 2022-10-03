package org.sirix.xquery;

import com.google.common.base.Preconditions;
import org.brackit.xquery.BrackitQueryContext;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.*;
import org.brackit.xquery.update.UpdateList;
import org.brackit.xquery.update.op.UpdateOp;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.json.JsonCollection;
import org.brackit.xquery.xdm.node.Node;
import org.brackit.xquery.xdm.node.NodeCollection;
import org.brackit.xquery.xdm.node.NodeFactory;
import org.brackit.xquery.xdm.type.ItemType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.xquery.json.BasicJsonDBStore;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonDBStore;
import org.sirix.xquery.node.BasicXmlDBStore;
import org.sirix.xquery.node.XmlDBNode;
import org.sirix.xquery.node.XmlDBStore;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Query context for Sirix.
 *
 * @author Johannes
 */
public final class SirixQueryContext implements QueryContext, AutoCloseable {

  /**
   * Commit strategies.
   */
  public enum CommitStrategy {
    /**
     * Automatically commit.
     */
    AUTO,

    /**
     * Explicitly commit (not within the applyUpdates-method).
     */
    EXPLICIT
  }

  /**
   * The commit strategy.
   */
  private final CommitStrategy commitStrategy;

  /**
   * The query context delegate.
   */
  private final QueryContext queryContextDelegate;

  /**
   * The node store (XML store).
   */
  private final XmlDBStore xmlStore;

  /**
   * The json item store.
   */
  private final JsonDBStore jsonStore;

  /**
   * The commit message if any.
   */
  private final String commitMessage;

  /**
   * The commit timestamp if any.
   */
  private final Instant commitTimestamp;

  public static SirixQueryContext createWithNodeStore(final XmlDBStore nodeStore) {
    return new SirixQueryContext(nodeStore, null, CommitStrategy.AUTO, null, null);
  }

  public static SirixQueryContext createWithNodeStoreAndCommitStrategy(final XmlDBStore nodeStore,
      final CommitStrategy commitStrategy) {
    return new SirixQueryContext(nodeStore, null, commitStrategy, null, null);
  }

  public static SirixQueryContext createWithJsonStore(final JsonDBStore jsonItemStore) {
    return new SirixQueryContext(null, jsonItemStore, CommitStrategy.AUTO, null, null);
  }

  public static SirixQueryContext createWithJsonStoreAndCommitStrategy(final JsonDBStore jsonItemStore,
      final CommitStrategy commitStrategy) {
    return new SirixQueryContext(null, jsonItemStore, commitStrategy, null, null);
  }

  public static SirixQueryContext createWithJsonStoreAndNodeStoreAndCommitStrategy(final XmlDBStore nodeStore,
      final JsonDBStore jsonItemStore, final CommitStrategy commitStrategy) {
    return new SirixQueryContext(nodeStore, jsonItemStore, commitStrategy, null, null);
  }

  public static SirixQueryContext createWithJsonStoreAndNodeStoreAndCommitStrategy(final XmlDBStore nodeStore,
      final JsonDBStore jsonItemStore, final CommitStrategy commitStrategy, final String commitMessage, final Instant commitTimestamp) {
    return new SirixQueryContext(nodeStore, jsonItemStore, commitStrategy, commitMessage, commitTimestamp);
  }


  public static SirixQueryContext create() {
    return new SirixQueryContext(null, null, CommitStrategy.AUTO, null, null);
  }

  /**
   * Private constructor.
   *
   * @param nodeStore      the database node storage to use
   * @param jsonItemStore  the database json item storage to use
   * @param commitStrategy the commit strategy to use
   */
  private SirixQueryContext(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore,
      final CommitStrategy commitStrategy, @Nullable final String commitMessage,
      @Nullable final Instant commitTimestamp) {
    xmlStore = nodeStore == null ? BasicXmlDBStore.newBuilder().build() : nodeStore;
    jsonStore = jsonItemStore == null ? BasicJsonDBStore.newBuilder().build() : jsonItemStore;
    queryContextDelegate = new BrackitQueryContext(nodeStore);
    this.commitStrategy = Preconditions.checkNotNull(commitStrategy);
    this.commitMessage = commitMessage;
    this.commitTimestamp = commitTimestamp;
  }

  @Override
  public void setDefaultJsonCollection(JsonCollection<?> defaultJsonCollection) {
    queryContextDelegate.setDefaultJsonCollection(defaultJsonCollection);
  }

  @Override
  public void setDefaultNodeCollection(NodeCollection<?> defaultNodeCollection) {
    queryContextDelegate.setDefaultNodeCollection(defaultNodeCollection);
  }

  @Override
  public void applyUpdates() {
    queryContextDelegate.applyUpdates();

    if (commitStrategy == CommitStrategy.AUTO) {
      final List<UpdateOp> updateList = queryContextDelegate.getUpdateList() == null
          ? Collections.emptyList()
          : queryContextDelegate.getUpdateList().list();

      if (!updateList.isEmpty()) {
        commitJsonTrx(updateList);
        commitXmlTrx(updateList);
        updateList.clear();
      }
    }
  }

  private void commitXmlTrx(List<UpdateOp> updateList) {
    final Function<Sequence, Optional<XmlNodeTrx>> mapDBNodeToWtx = sequence -> {
      if (sequence instanceof XmlDBNode) {
        return ((XmlDBNode) sequence).getTrx().getResourceSession().getNodeTrx();
      }

      return Optional.empty();
    };

    final var trxIDs = new HashSet<Long>();

    updateList.stream()
              .map(UpdateOp::getTarget)
              .map(mapDBNodeToWtx)
              .flatMap(Optional::stream)
              .filter(trx -> trxIDs.add(trx.getId()))
              .forEach(trx -> trx.commit(commitMessage, commitTimestamp));
  }

  private void commitJsonTrx(List<UpdateOp> updateList) {
    final Function<Sequence, Optional<JsonNodeTrx>> mapDBNodeToWtx = sequence -> {
      if (sequence instanceof JsonDBItem) {
        return ((JsonDBItem) sequence).getTrx().getResourceSession().getNodeTrx();
      }

      return Optional.empty();
    };

    final var trxIDs = new HashSet<Long>();

    updateList.stream()
              .map(UpdateOp::getTarget)
              .map(mapDBNodeToWtx)
              .flatMap(Optional::stream)
              .filter(trx -> trxIDs.add(trx.getId()))
              .forEach(trx -> trx.commit(commitMessage, commitTimestamp));
  }

  @Override
  public void addPendingUpdate(UpdateOp op) {
    queryContextDelegate.addPendingUpdate(op);
  }

  @Override
  public UpdateList getUpdateList() {
    return queryContextDelegate.getUpdateList();
  }

  @Override
  public void setUpdateList(UpdateList updates) {
    queryContextDelegate.setUpdateList(updates);
  }

  @Override
  public void bind(QNm name, Sequence sequence) {
    queryContextDelegate.bind(name, sequence);
  }

  @Override
  public Sequence resolve(QNm name) throws QueryException {
    return queryContextDelegate.resolve(name);
  }

  @Override
  public boolean isBound(QNm name) {
    return queryContextDelegate.isBound(name);
  }

  @Override
  public void setContextItem(Item item) {
    queryContextDelegate.setContextItem(item);
  }

  @Override
  public Item getContextItem() {
    return queryContextDelegate.getContextItem();
  }

  @Override
  public ItemType getItemType() {
    return queryContextDelegate.getItemType();
  }

  @Override
  public Node<?> getDefaultDocument() {
    return queryContextDelegate.getDefaultDocument();
  }

  @Override
  public void setDefaultDocument(Node<?> defaultDocument) {
    queryContextDelegate.setDefaultDocument(defaultDocument);
  }

  @Override
  public NodeCollection<?> getDefaultNodeCollection() {
    return queryContextDelegate.getDefaultNodeCollection();
  }

  @Override
  public JsonCollection<?> getDefaultJsonCollection() {
    return queryContextDelegate.getDefaultJsonCollection();
  }

  @Override
  public DateTime getDateTime() {
    return queryContextDelegate.getDateTime();
  }

  @Override
  public Date getDate() {
    return queryContextDelegate.getDate();
  }

  @Override
  public Time getTime() {
    return queryContextDelegate.getTime();
  }

  @Override
  public DTD getImplicitTimezone() {
    return queryContextDelegate.getImplicitTimezone();
  }

  @Override
  public AnyURI getBaseUri() {
    return queryContextDelegate.getBaseUri();
  }

  @Override
  public NodeFactory<?> getNodeFactory() {
    return queryContextDelegate.getNodeFactory();
  }

  @Override
  public XmlDBStore getNodeStore() {
    return xmlStore;
  }

  @Override
  public JsonDBStore getJsonItemStore() {
    return jsonStore;
  }

  public String getCommitMessage() {
    return commitMessage;
  }

  @Override
  public void close() {
    xmlStore.close();
    jsonStore.close();
  }
}
