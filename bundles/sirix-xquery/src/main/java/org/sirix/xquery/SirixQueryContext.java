package org.sirix.xquery;

import com.google.common.base.Preconditions;
import org.brackit.xquery.BrackitQueryContext;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Date;
import org.brackit.xquery.atomic.*;
import org.brackit.xquery.update.UpdateList;
import org.brackit.xquery.update.op.UpdateOp;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.node.Node;
import org.brackit.xquery.xdm.node.NodeCollection;
import org.brackit.xquery.xdm.node.NodeFactory;
import org.brackit.xquery.xdm.type.ItemType;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.xquery.json.BasicJsonDBStore;
import org.sirix.xquery.json.JsonDBStore;
import org.sirix.xquery.node.BasicXmlDBStore;
import org.sirix.xquery.node.XmlDBNode;
import org.sirix.xquery.node.XmlDBStore;

import java.util.*;
import java.util.function.Function;

/**
 * Query context for Sirix.
 *
 * @author Johannes
 *
 */
public final class SirixQueryContext implements QueryContext, AutoCloseable {

  /** Commit strategies. */
  public enum CommitStrategy {
    /** Automatically commit. */
    AUTO,

    /** Explicitly commit (not within the applyUpdates-method). */
    EXPLICIT
  }

  /** The commit strategy. */
  private final CommitStrategy mCommitStrategy;

  /** The query context delegate. */
  private final QueryContext mQueryContextDelegate;

  /** The node store (XML store). */
  private final XmlDBStore mXmlStore;

  /** The json item store. */
  private final JsonDBStore mJsonStore;

  public static final SirixQueryContext createWithNodeStore(final XmlDBStore nodeStore) {
    return new SirixQueryContext(nodeStore, null, CommitStrategy.AUTO);
  }

  public static final SirixQueryContext createWithNodeStoreAndCommitStrategy(final XmlDBStore nodeStore,
      final CommitStrategy commitStrategy) {
    return new SirixQueryContext(nodeStore, null, commitStrategy);
  }

  public static final SirixQueryContext createWithJsonStore(final JsonDBStore jsonItemStore) {
    return new SirixQueryContext(null, jsonItemStore, CommitStrategy.AUTO);
  }

  public static final SirixQueryContext createWithJsonStoreAndCommitStrategy(final JsonDBStore jsonItemStore,
      final CommitStrategy commitStrategy) {
    return new SirixQueryContext(null, jsonItemStore, commitStrategy);
  }

  public static final SirixQueryContext createWithJsonStoreAndNodeStoreAndCommitStrategy(
      final JsonDBStore jsonItemStore, final CommitStrategy commitStrategy) {
    return new SirixQueryContext(null, jsonItemStore, commitStrategy);
  }

  public static final SirixQueryContext create() {
    return new SirixQueryContext(null, null, CommitStrategy.AUTO);
  }

  /**
   * Private constructor.
   *
   * @param nodeStore the database node storage to use
   * @param jsonStore the database json item storage to use
   * @param commitStrategy the commit strategy to use
   */
  private SirixQueryContext(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore,
      final CommitStrategy commitStrategy) {
    mXmlStore = nodeStore == null
        ? BasicXmlDBStore.newBuilder().build()
        : nodeStore;
    mJsonStore = jsonItemStore == null
        ? BasicJsonDBStore.newBuilder().build()
        : jsonItemStore;
    mQueryContextDelegate = new BrackitQueryContext(nodeStore);
    mCommitStrategy = Preconditions.checkNotNull(commitStrategy);
  }

  @Override
  public void applyUpdates() {
    mQueryContextDelegate.applyUpdates();

    if (mCommitStrategy == CommitStrategy.AUTO) {
      final List<UpdateOp> updateList = mQueryContextDelegate.getUpdateList() == null
          ? Collections.emptyList()
          : mQueryContextDelegate.getUpdateList().list();

      if (!updateList.isEmpty()) {
        final Function<Sequence, Optional<XmlNodeTrx>> mapDBNodeToWtx = sequence -> {
          if (sequence instanceof XmlDBNode) {
            return ((XmlDBNode) sequence).getTrx().getResourceManager().getNodeWriteTrx();
          }

          return Optional.empty();
        };

        final Set<Long> trxIDs = new HashSet<>();

        updateList.stream()
                  .map(UpdateOp::getTarget)
                  .map(mapDBNodeToWtx)
                  .flatMap(Optional::stream)
                  .filter(trx -> trxIDs.add(trx.getId()))
                  .forEach(XmlNodeTrx::commit);
      }
    }
  }

  @Override
  public void addPendingUpdate(UpdateOp op) {
    mQueryContextDelegate.addPendingUpdate(op);
  }

  @Override
  public UpdateList getUpdateList() {
    return mQueryContextDelegate.getUpdateList();
  }

  @Override
  public void setUpdateList(UpdateList updates) {
    mQueryContextDelegate.setUpdateList(updates);
  }

  @Override
  public void bind(QNm name, Sequence sequence) {
    mQueryContextDelegate.bind(name, sequence);
  }

  @Override
  public Sequence resolve(QNm name) throws QueryException {
    return mQueryContextDelegate.resolve(name);
  }

  @Override
  public boolean isBound(QNm name) {
    return mQueryContextDelegate.isBound(name);
  }

  @Override
  public void setContextItem(Item item) {
    mQueryContextDelegate.setContextItem(item);
  }

  @Override
  public Item getContextItem() {
    return mQueryContextDelegate.getContextItem();
  }

  @Override
  public ItemType getItemType() {
    return mQueryContextDelegate.getItemType();
  }

  @Override
  public Node<?> getDefaultDocument() {
    return mQueryContextDelegate.getDefaultDocument();
  }

  @Override
  public void setDefaultDocument(Node<?> defaultDocument) {
    mQueryContextDelegate.setDefaultDocument(defaultDocument);
  }

  @Override
  public NodeCollection<?> getDefaultCollection() {
    return mQueryContextDelegate.getDefaultCollection();
  }

  @Override
  public void setDefaultCollection(NodeCollection<?> defaultCollection) {
    mQueryContextDelegate.setDefaultCollection(defaultCollection);
  }

  @Override
  public DateTime getDateTime() {
    return mQueryContextDelegate.getDateTime();
  }

  @Override
  public Date getDate() {
    return mQueryContextDelegate.getDate();
  }

  @Override
  public Time getTime() {
    return mQueryContextDelegate.getTime();
  }

  @Override
  public DTD getImplicitTimezone() {
    return mQueryContextDelegate.getImplicitTimezone();
  }

  @Override
  public AnyURI getBaseUri() {
    return mQueryContextDelegate.getBaseUri();
  }

  @Override
  public NodeFactory<?> getNodeFactory() {
    return mQueryContextDelegate.getNodeFactory();
  }

  @Override
  public XmlDBStore getNodeStore() {
    return mXmlStore;
  }

  @Override
  public JsonDBStore getJsonItemStore() {
    return mJsonStore;
  }

  @Override
  public void close() {
    mXmlStore.close();
    mJsonStore.close();
  }
}
