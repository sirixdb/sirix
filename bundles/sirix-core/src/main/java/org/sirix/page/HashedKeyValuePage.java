package org.sirix.page;

import com.google.common.collect.Iterators;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.node.DeweyIDNode;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NodePersistenter;
import org.sirix.node.interfaces.RecordPersister;
import org.sirix.page.interfaces.KeyValuePage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;

import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;

public final class HashedKeyValuePage<K extends Comparable<? super K>> implements KeyValuePage<K, DataRecord> {
  private final Map<K, DataRecord> records;
  private final int revision;
  private final long recordPageKey;
  private final PageReadOnlyTrx pageReadOnlyTrx;
  private final PageKind pageKind;
  private final ResourceConfiguration resourceConfig;
  private final RecordPersister recordPersister;

  /**
   * Copy constructor.
   *
   * @param pageReadOnlyTrx the page read-only trx
   * @param pageToClone     the page to clone
   */
  public HashedKeyValuePage(final PageReadOnlyTrx pageReadOnlyTrx, final HashedKeyValuePage<K> pageToClone) {
    resourceConfig = pageReadOnlyTrx.getResourceManager().getResourceConfig();
    this.pageReadOnlyTrx = pageToClone.pageReadOnlyTrx;
    records = pageToClone.records;
    revision = pageToClone.revision;
    recordPageKey = pageToClone.recordPageKey;
    pageKind = pageToClone.pageKind;
    recordPersister = pageToClone.recordPersister;
  }

  /**
   * Constructor which initializes a new {@link UnorderedKeyValuePage}.
   *
   * @param recordPageKey   base key assigned to this node page
   * @param pageKind        the kind of subtree page (NODEPAGE, PATHSUMMARYPAGE, TEXTVALUEPAGE,
   *                        ATTRIBUTEVALUEPAGE...)
   * @param pageReadOnlyTrx the page reading transaction
   */
  public HashedKeyValuePage(@Nonnegative final long recordPageKey, final PageKind pageKind,
      final PageReadOnlyTrx pageReadOnlyTrx) {
    // Assertions instead of checkNotNull(...) checks as it's part of the
    // internal flow.
    assert recordPageKey >= 0 : "recordPageKey must not be negative!";
    assert pageReadOnlyTrx != null : "The page reading trx must not be null!";

    resourceConfig = pageReadOnlyTrx.getResourceManager().getResourceConfig();
    recordPersister = resourceConfig.recordPersister;
    this.recordPageKey = recordPageKey;
    records = new LinkedHashMap<>();
    this.pageReadOnlyTrx = pageReadOnlyTrx;
    revision = pageReadOnlyTrx.getRevisionNumber();
    this.pageKind = pageKind;
  }

  /**
   * Constructor which reads the {@link HashedKeyValuePage} from the storage.
   *
   * @param in              input bytes to read page from
   * @param pageReadOnlyTrx {@link PageReadOnlyTrx} implementation
   */
  protected HashedKeyValuePage(final DataInput in, final PageReadOnlyTrx pageReadOnlyTrx) throws IOException {
    this.pageReadOnlyTrx = pageReadOnlyTrx;

    resourceConfig = pageReadOnlyTrx.getResourceManager().getResourceConfig();
    recordPersister = resourceConfig.recordPersister;
    revision = pageReadOnlyTrx.getRevisionNumber();

    final var size = in.readInt();
    records = new LinkedHashMap<>(size);
    pageKind = PageKind.getKind(in.readByte());
    recordPageKey = getVarLong(in);

    if (pageKind == PageKind.DEWEYIDPAGE && resourceConfig.areDeweyIDsStored
        && recordPersister instanceof NodePersistenter persistenter) {
      SirixDeweyID optionalDeweyId = null;

      for (int index = 0; index < size; index++) {
        optionalDeweyId = persistenter.deserializeDeweyID(in, optionalDeweyId, resourceConfig);

        if (optionalDeweyId != null) {
          deserializeRecordAndPutIntoMap(in, optionalDeweyId);
        }
      }
    }
  }

  private void deserializeRecordAndPutIntoMap(DataInput in, SirixDeweyID deweyId) {
    try {
      final long nodeKey = getVarLong(in);
      records.put((K) deweyId, new DeweyIDNode(nodeKey, deweyId));
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Set<Map.Entry<K, DataRecord>> entrySet() {
    return records.entrySet();
  }

  @Override
  public Collection<DataRecord> values() {
    return records.values();
  }

  @Override
  public long getPageKey() {
    return recordPageKey;
  }

  @Override
  public DataRecord getValue(K key) {
    return records.get(key);
  }

  @Override
  public void setRecord(K key, @Nonnull DataRecord value) {
    records.put(key, value);
  }

  @Override
  public Set<Map.Entry<K, PageReference>> referenceEntrySet() {
    return Collections.emptySet();
  }

  @Override
  public void setPageReference(K key, @Nonnull PageReference reference) {
  }

  @Override
  public PageReference getPageReference(K key) {
    return null;
  }

  @Override
  public <C extends KeyValuePage<K, DataRecord>> C newInstance(long recordPageKey, @Nonnull PageKind pageKind,
      @Nonnull PageReadOnlyTrx pageReadOnlyTrx) {
    return (C) new HashedKeyValuePage<K>(recordPageKey, pageKind, pageReadOnlyTrx);
  }

  @Override
  public PageReadOnlyTrx getPageReadOnlyTrx() {
    return pageReadOnlyTrx;
  }

  @Override
  public PageKind getPageKind() {
    return PageKind.DEWEYIDPAGE;
  }

  @Override
  public int size() {
    return records.size();
  }

  @Override
  public int getRevision() {
    return revision;
  }

  @Override
  public void serialize(DataOutput out, SerializationType type) throws IOException {
    out.writeInt(records.size());
    out.writeByte(pageKind.getID());
    putVarLong(out, recordPageKey);

    // Check for dewey IDs.
    if (pageKind == PageKind.DEWEYIDPAGE && resourceConfig.areDeweyIDsStored
        && recordPersister instanceof NodePersistenter persistence && !records.isEmpty()) {
      final Set<K> recordKeys = records.keySet();
      final K firstRecord = recordKeys.iterator().next();

      if (firstRecord instanceof SirixDeweyID) {
        // Write dewey IDs.
        final List<SirixDeweyID> ids = new ArrayList<>((Collection<? extends SirixDeweyID>) recordKeys);
        ids.sort(Comparator.comparingInt((SirixDeweyID sirixDeweyID) -> sirixDeweyID.toBytes().length));
        final var iter = Iterators.peekingIterator(ids.iterator());
        SirixDeweyID id = null;
        if (iter.hasNext()) {
          id = iter.next();
          persistence.serializeDeweyID(out, id, null, resourceConfig);
          serializeDeweyRecord(id, out);
        }
        while (iter.hasNext()) {
          final var nextDeweyID = iter.next();
          persistence.serializeDeweyID(out, id, nextDeweyID, resourceConfig);
          serializeDeweyRecord(nextDeweyID, out);
          id = nextDeweyID;
        }
      }
    }
  }

  private void serializeDeweyRecord(SirixDeweyID id, DataOutput out) throws IOException {
    final DeweyIDNode node = (DeweyIDNode) records.get(id);
    final long nodeKey = node.getNodeKey();
    putVarLong(out, nodeKey);
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(@Nonnull PageTrx pageTrx) {
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }
}
