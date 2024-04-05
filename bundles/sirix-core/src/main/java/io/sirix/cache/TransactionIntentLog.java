package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import java.util.ArrayList;
import java.util.List;

public final class TransactionIntentLog implements AutoCloseable {

  private final Long2ObjectMap<PageContainer> map;
  private final BufferManager bufferManager;

  private volatile int logKey;

  public TransactionIntentLog(final BufferManager bufferManager, final int maxInMemoryCapacity, final int initialLogKey) {
    this.bufferManager = bufferManager;
    logKey = initialLogKey;
    map = new Long2ObjectArrayMap<>(maxInMemoryCapacity);
  }

  // Copy constructor
  public TransactionIntentLog(final TransactionIntentLog original) {
    this.bufferManager = original.bufferManager;
    this.logKey = original.logKey;
    this.map = new Long2ObjectArrayMap<>(original.map);
  }

  public PageContainer get(final PageReference key) {
    var logKey = key.getLogKey();
    if ((logKey >= this.logKey) || logKey < 0) {
      return null;
    }
    return map.get(logKey);
  }

  public void put(final PageReference key, final PageContainer value) {
    bufferManager.getRecordPageCache().remove(key);
    bufferManager.getPageCache().remove(key);

    if (key.getLogKey() == Constants.NULL_ID_INT) {
      key.setLogKey(logKey);

      map.put(logKey, value);
      logKey++;
    } else {
      map.put(key.getLogKey(), value);
    }
  }

  public void clear() {
    logKey = 0;
    map.clear();
  }

  public List<PageContainer> getList() {
    return new ArrayList<>(map.values());
  }

  @Override
  public void close() {
    logKey = 0;
    map.values()
       .stream()
       .filter(pageContainer -> pageContainer.getComplete() instanceof KeyValueLeafPage)
       .forEach(pageContainer -> {
         pageContainer.getModifiedAsUnorderedKeyValuePage().clearPage();
         pageContainer.getCompleteAsUnorderedKeyValuePage().clearPage();
       });
    map.clear();
  }

  public int getMaxLogKey() {
    return logKey - 1;
  }

  public void remove(PageReference reference) {
    map.remove(reference.getLogKey());
  }
}