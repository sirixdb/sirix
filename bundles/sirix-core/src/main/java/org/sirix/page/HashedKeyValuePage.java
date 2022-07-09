package org.sirix.page;

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.node.DeweyIDNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NodePersistenter;
import org.sirix.node.interfaces.RecordSerializer;
import org.sirix.page.interfaces.KeyValuePage;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;

public final class HashedKeyValuePage<K extends Comparable<? super K>> implements KeyValuePage<K, DataRecord> {
  private final Map<K, DataRecord> records;
  private final int revision;
  private final long recordPageKey;
  private final PageReadOnlyTrx pageReadOnlyTrx;
  private final IndexType indexType;
  private final ResourceConfiguration resourceConfig;
  private final RecordSerializer recordPersister;

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
    indexType = pageToClone.indexType;
    recordPersister = pageToClone.recordPersister;
  }

  /**
   * Constructor which initializes a new {@link UnorderedKeyValuePage}.
   *
   * @param recordPageKey   base key assigned to this node page
   * @param indexType       the index type
   * @param pageReadOnlyTrx the page reading transaction
   */
  public HashedKeyValuePage(@NonNegative final long recordPageKey, final IndexType indexType,
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
    this.indexType = indexType;
  }

  /**
   * Constructor which reads the {@link HashedKeyValuePage} from the storage.
   *
   * @param in              input bytes to read page from
   * @param pageReadOnlyTrx {@link PageReadOnlyTrx} implementation
   */
  HashedKeyValuePage(final Bytes<ByteBuffer> in, final PageReadOnlyTrx pageReadOnlyTrx) {
    this.pageReadOnlyTrx = pageReadOnlyTrx;

    resourceConfig = pageReadOnlyTrx.getResourceManager().getResourceConfig();
    recordPersister = resourceConfig.recordPersister;
    revision = pageReadOnlyTrx.getRevisionNumber();

    final var size = in.readInt();
    records = new LinkedHashMap<>(size);
    indexType = IndexType.getType(in.readByte());
    recordPageKey = getVarLong(in);

    if (indexType == IndexType.DEWEYID_TO_RECORDID && resourceConfig.areDeweyIDsStored
        && recordPersister instanceof NodePersistenter persistenter) {
      byte[] optionalDeweyId = null;

      for (int index = 0; index < size; index++) {
        final byte[] deweyID = persistenter.deserializeDeweyID(in, optionalDeweyId, resourceConfig);

        optionalDeweyId = deweyID;

        if (deweyID != null) {
          deserializeRecordAndPutIntoMap(in, new SirixDeweyID(deweyID));
        }
      }
    }
  }

  private void deserializeRecordAndPutIntoMap(Bytes<ByteBuffer> in, SirixDeweyID deweyId) {
    final long nodeKey = getVarLong(in);
    records.put((K) deweyId, new DeweyIDNode(nodeKey, deweyId));
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
  public void setRecord(K key, @NonNull DataRecord value) {
    records.put(key, value);
  }

  @Override
  public Set<Map.Entry<K, PageReference>> referenceEntrySet() {
    return Collections.emptySet();
  }

  @Override
  public void setPageReference(K key, @NonNull PageReference reference) {
  }

  @Override
  public PageReference getPageReference(K key) {
    return null;
  }

  @Override
  public <C extends KeyValuePage<K, DataRecord>> C newInstance(long recordPageKey, @NonNull IndexType indexType,
      @NonNull PageReadOnlyTrx pageReadOnlyTrx) {
    return (C) new HashedKeyValuePage<K>(recordPageKey, indexType, pageReadOnlyTrx);
  }

  @Override
  public PageReadOnlyTrx getPageReadOnlyTrx() {
    return pageReadOnlyTrx;
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.DEWEYID_TO_RECORDID;
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
  public void serialize(Bytes<ByteBuffer> out, SerializationType type) {
    out.writeInt(records.size());
    out.writeByte(indexType.getID());
    putVarLong(out, recordPageKey);

    // Check for dewey IDs.
    if (indexType == IndexType.DEWEYID_TO_RECORDID && resourceConfig.areDeweyIDsStored
        && recordPersister instanceof NodePersistenter persistence && !records.isEmpty()) {
      final Set<K> recordKeys = records.keySet();
      final K firstRecord = recordKeys.iterator().next();

      if (firstRecord instanceof SirixDeweyID) {
        //        // Write dewey IDs.
        //        final List<SirixDeweyID> ids = new ArrayList<>((Collection<? extends SirixDeweyID>) recordKeys);
        //        final List<byte[]> sirixDeweyIds = ids.stream().map(SirixDeweyID::toBytes).collect(Collectors.toList());
        //        sirixDeweyIds.sort(Comparator.comparingInt((byte[] sirixDeweyID) -> sirixDeweyID.length));
        //        final var iter = Iterators.peekingIterator(sirixDeweyIds.iterator());
        //        byte[] id = null;
        //        if (iter.hasNext()) {
        //          id = iter.next();
        //          persistence.serializeDeweyID(out, id, null, resourceConfig);
        //          serializeDeweyRecord(id, out);
        //        }
        //        while (iter.hasNext()) {
        //          final var nextDeweyID = iter.next();
        //          persistence.serializeDeweyID(out, id, nextDeweyID, resourceConfig);
        //          serializeDeweyRecord(nextDeweyID, out);
        //          id = nextDeweyID;
        //        }
      }
    }
  }

  private void serializeDeweyRecord(SirixDeweyID id, Bytes<ByteBuffer> out) {
    final DeweyIDNode node = (DeweyIDNode) records.get(id);
    final long nodeKey = node.getNodeKey();
    putVarLong(out, nodeKey);
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(@NonNull PageTrx pageTrx) {
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
