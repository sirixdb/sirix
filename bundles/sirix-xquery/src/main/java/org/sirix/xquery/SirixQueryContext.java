package org.sirix.xquery;

import java.util.*;
import java.util.function.Function;
import org.brackit.xquery.BrackitQueryContext;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.AnyURI;
import org.brackit.xquery.atomic.DTD;
import org.brackit.xquery.atomic.Date;
import org.brackit.xquery.atomic.DateTime;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Time;
import org.brackit.xquery.update.UpdateList;
import org.brackit.xquery.update.op.UpdateOp;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.node.Node;
import org.brackit.xquery.xdm.node.NodeCollection;
import org.brackit.xquery.xdm.node.NodeFactory;
import org.brackit.xquery.xdm.type.ItemType;
import org.sirix.access.User;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.xquery.json.BasicJsonDBStore;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonDBStore;
import org.sirix.xquery.node.BasicXmlDBStore;
import org.sirix.xquery.node.XmlDBNode;
import org.sirix.xquery.node.XmlDBStore;
import com.google.common.base.Preconditions;

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
  private final CommitStrategy commitStrategy;

  /** The query context delegate. */
  private final QueryContext queryContextDelegate;

  /** The node store (XML store). */
  private final XmlDBStore xmlStore;

  /** The json item store. */
  private final JsonDBStore jsonStore;

  private final Map<String, Object> properties;

  public static SirixQueryContext createWithNodeStore(final XmlDBStore nodeStore) {
    return new SirixQueryContext(nodeStore, null, CommitStrategy.AUTO);
  }

  public static SirixQueryContext createWithNodeStoreAndCommitStrategy(final XmlDBStore nodeStore,
      final CommitStrategy commitStrategy) {
    return new SirixQueryContext(nodeStore, null, commitStrategy);
  }

  public static SirixQueryContext createWithJsonStore(final JsonDBStore jsonItemStore) {
    return new SirixQueryContext(null, jsonItemStore, CommitStrategy.AUTO);
  }

  public static SirixQueryContext createWithJsonStoreAndCommitStrategy(final JsonDBStore jsonItemStore,
      final CommitStrategy commitStrategy) {
    return new SirixQueryContext(null, jsonItemStore, commitStrategy);
  }

  public static SirixQueryContext createWithJsonStoreAndNodeStoreAndCommitStrategy(final XmlDBStore nodeStore,
      final JsonDBStore jsonItemStore, final CommitStrategy commitStrategy) {
    return new SirixQueryContext(nodeStore, jsonItemStore, commitStrategy);
  }

  public static SirixQueryContext create() {
    return new SirixQueryContext(null, null, CommitStrategy.AUTO);
  }

  /**
   * Private constructor.
   *
   * @param nodeStore the database node storage to use
   * @param jsonItemStore the database json item storage to use
   * @param commitStrategy the commit strategy to use
   */
  private SirixQueryContext(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore,
      final CommitStrategy commitStrategy) {
    xmlStore = nodeStore == null
        ? BasicXmlDBStore.newBuilder().build()
        : nodeStore;
    jsonStore = jsonItemStore == null
        ? BasicJsonDBStore.newBuilder().build()
        : jsonItemStore;
    queryContextDelegate = new BrackitQueryContext(nodeStore);
    this.commitStrategy = Preconditions.checkNotNull(commitStrategy);
    properties = new HashMap<>();
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
        return ((XmlDBNode) sequence).getTrx().getResourceManager().getNodeTrx();
      }

      return Optional.empty();
    };

    final var trxIDs = new HashSet<Long>();

    updateList.stream()
              .map(UpdateOp::getTarget)
              .map(mapDBNodeToWtx)
              .flatMap(Optional::stream)
              .filter(trx -> trxIDs.add(trx.getId()))
              .forEach(XmlNodeTrx::commit);
  }

  private void commitJsonTrx(List<UpdateOp> updateList) {
    final Function<Sequence, Optional<JsonNodeTrx>> mapDBNodeToWtx = sequence -> {
      if (sequence instanceof JsonDBItem) {
        return ((JsonDBItem) sequence).getTrx().getResourceManager().getNodeTrx();
      }

      return Optional.empty();
    };

    final var trxIDs = new HashSet<Long>();

    updateList.stream()
              .map(UpdateOp::getTarget)
              .map(mapDBNodeToWtx)
              .flatMap(Optional::stream)
              .filter(trx -> trxIDs.add(trx.getId()))
              .forEach(JsonNodeTrx::commit);
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
  public NodeCollection<?> getDefaultCollection() {
    return queryContextDelegate.getDefaultCollection();
  }

  @Override
  public void setDefaultCollection(NodeCollection<?> defaultCollection) {
    queryContextDelegate.setDefaultCollection(defaultCollection);
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

  @Override
  public void close() {
    xmlStore.close();
    jsonStore.close();
  }

  public SirixQueryContext addProperty(String key, Object value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    properties.put(key, value);
    return this;
  }

  public Map<String, Object> getProperties() {
    return Collections.unmodifiableMap(properties);
  }
}
