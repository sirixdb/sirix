package io.sirix.query;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.AnyURI;
import io.brackit.query.atomic.DTD;
import io.brackit.query.atomic.Date;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Time;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.JsonCollection;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.jdm.node.NodeCollection;
import io.brackit.query.jdm.node.NodeFactory;
import io.brackit.query.jdm.type.ItemType;
import io.brackit.query.update.UpdateList;
import io.brackit.query.update.op.UpdateOp;
import io.brackit.query.update.op.OpType;
import io.brackit.query.jdm.StructuredItem;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.NodeTrx;
import io.sirix.query.compiler.optimizer.PlanCache;
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBNode;
import io.sirix.query.node.XmlDBStore;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Query context for Sirix.
 *
 * @author Johannes
 */
public final class SirixQueryContext implements QueryContext, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SirixQueryContext.class);

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

  private @Nullable DateTime dateTime;

  private @Nullable Date date;

  private @Nullable Time time;

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
      final JsonDBStore jsonItemStore, final CommitStrategy commitStrategy, final String commitMessage,
      final Instant commitTimestamp) {
    return new SirixQueryContext(nodeStore, jsonItemStore, commitStrategy, commitMessage, commitTimestamp);
  }


  public static SirixQueryContext create() {
    return new SirixQueryContext(null, null, CommitStrategy.AUTO, null, null);
  }

  /**
   * Private constructor.
   *
   * @param nodeStore the database node storage to use
   * @param jsonItemStore the database json item storage to use
   * @param commitStrategy the commit strategy to use
   */
  private SirixQueryContext(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore,
      final CommitStrategy commitStrategy, @Nullable final String commitMessage,
      @Nullable final Instant commitTimestamp) {
    xmlStore = nodeStore == null
        ? BasicXmlDBStore.newBuilder().build()
        : nodeStore;
    jsonStore = jsonItemStore == null
        ? BasicJsonDBStore.newBuilder().build()
        : jsonItemStore;
    queryContextDelegate = new BrackitQueryContext(nodeStore);
    this.commitStrategy = requireNonNull(commitStrategy);
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
    final UpdateList pending = queryContextDelegate.getUpdateList();
    if (pending == null || pending.list().isEmpty()) {
      queryContextDelegate.applyUpdates();
      return;
    }
    boolean hasXmlTargets = false;
    for (final UpdateOp operation : pending.list()) {
      if (operation.getTarget() instanceof XmlDBNode) {
        hasXmlTargets = true;
        break;
      }
    }
    final List<XmlNodeTrx> xmlWriters;
    if (hasXmlTargets) {
      xmlWriters = applyXmlUpdates(pending.list());
    } else {
      queryContextDelegate.applyUpdates();
      xmlWriters = Collections.emptyList();
    }

    if (commitStrategy == CommitStrategy.AUTO) {
      final List<UpdateOp> updateList = queryContextDelegate.getUpdateList() == null
          ? Collections.emptyList()
          : queryContextDelegate.getUpdateList().list();

      if (!updateList.isEmpty()) {
        commitJsonTrx(updateList);
        commitXmlTrx(xmlWriters);
        updateList.clear();
      }
    }
  }

  /** Hold every XML writer through operation application and final text normalization. */
  private List<XmlNodeTrx> applyXmlUpdates(final List<UpdateOp> operations) {
    final List<XmlNodeTrx> writers = new ArrayList<>(1);
    final Map<XmlResourceId, XmlNodeTrx> suppliedWriters = HashMap.newHashMap(1);
    final Map<XmlResourceId, XmlNodeTrx> writersByResource = HashMap.newHashMap(1);
    final List<UpdateOp> originals = new ArrayList<>(operations);
    try {
      for (final UpdateOp operation : originals) {
        if (operation.getTarget() instanceof XmlDBNode source) {
          final XmlNodeReadOnlyTrx reader = source.getTrx();
          final XmlNodeTrx writer = reader.getResourceSession().getNodeTrx().orElse(null);
          if (writer != null) {
            suppliedWriters.putIfAbsent(XmlResourceId.of(reader), writer);
          }
        }
      }
      for (final UpdateOp operation : originals) {
        if (operation.getTarget() instanceof XmlDBNode source) {
          final XmlNodeReadOnlyTrx reader = source.getTrx();
          final XmlResourceSession session = reader.getResourceSession();
          final XmlResourceId resource = XmlResourceId.of(reader);
          if (!writersByResource.containsKey(resource)) {
            final XmlNodeTrx supplied = suppliedWriters.get(resource);
            final boolean created = supplied == null;
            final XmlNodeTrx writer = created ? session.beginNodeTrx() : supplied;
            writer.beginAtomicOperation();
            writers.add(writer);
            writersByResource.put(resource, writer);
            if (created && reader.getRevisionNumber() < session.getMostRecentRevisionNumber()) {
              writer.revertTo(reader.getRevisionNumber());
            }
          }
        }
      }
      for (int i = 0; i < originals.size(); i++) {
        final UpdateOp operation = originals.get(i);
        if (operation.getTarget() instanceof XmlDBNode source) {
          final XmlNodeTrx writer = writersByResource.get(XmlResourceId.of(source.getTrx()));
          operations.set(i, new XmlNodeUpdate(operation, source, source.writerView(writer)));
        }
      }
      queryContextDelegate.applyUpdates();
    } catch (final RuntimeException | Error failure) {
      for (final XmlNodeTrx writer : writers) {
        writer.markRollbackOnly(failure);
      }
      throw failure;
    } finally {
      // Keep original snapshot identities and targets available to the caller after application.
      operations.clear();
      operations.addAll(originals);
      for (int i = writers.size() - 1; i >= 0; i--) {
        writers.get(i).endAtomicOperation();
      }
    }
    return writers;
  }

  private record XmlResourceId(long databaseId, long resourceId) {
    private static XmlResourceId of(final XmlNodeReadOnlyTrx reader) {
      final ResourceConfiguration configuration = reader.getResourceSession().getResourceConfig();
      return new XmlResourceId(configuration.getDatabaseId(), configuration.getID());
    }
  }

  private void commitXmlTrx(final List<XmlNodeTrx> writers) {
    for (final XmlNodeTrx writer : writers) {
      writer.commit(commitMessage, commitTimestamp);
      invalidateStatisticsForResource(writer);
      writer.close();
    }
  }

  private void commitJsonTrx(List<UpdateOp> updateList) {
    final Function<Sequence, Optional<JsonNodeTrx>> mapDBNodeToWtx = sequence -> {
      if (sequence instanceof JsonDBItem jsonItem) {
        return jsonItem.getTrx().getResourceSession().getNodeTrx();
      }
      return Optional.empty();
    };

    final var trxIDs = new IntArraySet();

    updateList.stream()
              .map(UpdateOp::getTarget)
              .map(mapDBNodeToWtx)
              .flatMap(Optional::stream)
              .filter(trx -> trxIDs.add(trx.getId()))
              .toList()
              .forEach(trx -> {
                trx.commit(commitMessage, commitTimestamp);
                invalidateStatisticsForResource(trx);
                trx.close();
              });
  }

  /**
   * Invalidate stale statistics and cached plans after a commit. Must never throw — statistics
   * cleanup must not fail the commit.
   */
  private static void invalidateStatisticsForResource(NodeTrx trx) {
    try {
      final var resourceConfig = trx.getResourceSession().getResourceConfig();
      final String resourceName = resourceConfig.getName();
      // Database name is the parent of the 'resources' directory in the resource path
      final var resourcePath = resourceConfig.getResource();
      final String databaseName = resourcePath.getParent().getParent().getFileName().toString();
      StatisticsCatalog.getInstance().invalidate(databaseName, resourceName);
      PlanCache.signalIndexSchemaChange();
    } catch (Exception e) {
      LOG.debug("Statistics invalidation after commit failed: {}", e.getMessage());
    }
  }

  @Override
  public void addPendingUpdate(UpdateOp op) {
    queryContextDelegate.addPendingUpdate(op);
  }

  /** Private live targets let Brackit normalize the completed writer tree, not a cached revision. */
  private record XmlNodeUpdate(UpdateOp delegate, XmlDBNode source, XmlDBNode target) implements UpdateOp {
    @Override
    public StructuredItem getTarget() {
      return target;
    }

    @Override
    public Object getTargetIdentity() {
      return delegate.getTargetIdentity();
    }

    @Override
    public OpType getType() {
      return delegate.getType();
    }

    @Override
    public void apply() {
      source.applyUpdate(delegate, target);
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
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
    // A document bound here is the only place a query using `declare variable $doc external` ever
    // names its resource — the query text does not, and no compile-time walk can find it. Recorded
    // so a store-only compile chain can still wire the analytical fast paths; see BoundDocumentHint
    // for why a hint that turns out to name the wrong document costs nothing but the fast path.
    BoundDocumentHint.remember(sequence);
  }

  @Override
  public Sequence resolve(QNm name) throws QueryException {
    return XmlDBNode.readView(queryContextDelegate.resolve(name));
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
    return XmlDBNode.readView(queryContextDelegate.getContextItem());
  }

  @Override
  public ItemType getItemType() {
    return queryContextDelegate.getItemType();
  }

  @Override
  public Node<?> getDefaultDocument() {
    final Node<?> document = queryContextDelegate.getDefaultDocument();
    return document instanceof XmlDBNode node ? node.readView() : document;
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
    if (dateTime == null) {
      final OffsetDateTime now = OffsetDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
      final DTD timezone = timezoneFromOffset(now.getOffset());
      dateTime = new DateTime((short) now.getYear(), (byte) now.getMonthValue(), (byte) now.getDayOfMonth(),
          (byte) now.getHour(), (byte) now.getMinute(), now.getSecond() * 1_000_000 + now.getNano() / 1_000, timezone);
    }
    return dateTime;
  }

  static DTD timezoneFromOffset(final ZoneOffset offset) {
    final int offsetSeconds = requireNonNull(offset, "offset").getTotalSeconds();
    final int magnitude = Math.abs(offsetSeconds);
    // DTD encodes its sign separately from the hour and minute magnitudes.
    return new DTD(offsetSeconds < 0, 0, (byte) (magnitude / 3600), (byte) (magnitude / 60 % 60),
        magnitude % 60 * 1_000_000);
  }

  @Override
  public Date getDate() {
    if (date == null) {
      date = new Date(getDateTime());
    }
    return date;
  }

  @Override
  public Time getTime() {
    if (time == null) {
      time = new Time(getDateTime());
    }
    return time;
  }

  @Override
  public DTD getImplicitTimezone() {
    return getDateTime().getTimezone();
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
