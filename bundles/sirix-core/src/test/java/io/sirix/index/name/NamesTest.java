package io.sirix.index.name;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.HashCountEntryNode;
import io.sirix.node.HashEntryNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotEquals;

public final class NamesTest {
  @Test
  public void whenIndexExistsForAnotherString_createNewIndex() {
    final StorageEngineWriter storageEngineWriter = createPageTrxMock("FB");

    final var names = Names.getInstance(0);

    final var fbIndex = names.setName("FB", storageEngineWriter);
    final var EaIndex = names.setName("Ea", storageEngineWriter);

    assertNotEquals(fbIndex, EaIndex);
  }

  private StorageEngineWriter createPageTrxMock(String name) {
    final var hashEntryNode = new HashEntryNode(2, 12, name);
    final var hashCountEntryNode = new HashCountEntryNode(3, 1);

    @SuppressWarnings("unchecked")
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.createRecord(any(HashEntryNode.class), eq(IndexType.NAME), eq(0))).thenReturn(hashEntryNode);
    when(storageEngineWriter.createRecord(any(HashCountEntryNode.class), eq(IndexType.NAME), eq(0))).thenReturn(hashCountEntryNode);
    when(storageEngineWriter.prepareRecordForModification(2L, IndexType.NAME, 0)).thenReturn(hashCountEntryNode);
    return storageEngineWriter;
  }

  @Test
  public void whenIndexExistsForSameString_createNoNewIndex() {
    final StorageEngineWriter storageEngineWriter = createPageTrxMock("FB");

    final var names = Names.getInstance(0);

    final var fbIndex = names.setName("FB", storageEngineWriter);
    final var EaIndex = names.setName("FB", storageEngineWriter);

    assertEquals(fbIndex, EaIndex);
  }

  @Test
  public void whenIndexExistsForSameString_increaseCounter() {
    final StorageEngineWriter storageEngineWriter = createPageTrxMock("FB");

    final var names = Names.getInstance(0);

    final var index = names.setName("FB", storageEngineWriter);
    names.setName("FB", storageEngineWriter);

    assertEquals(2, names.getCount(index));
  }

  @Test
  public void whenIndexDoesNotExistsForString_counterIsZero() {
    final var names = Names.getInstance(0);

    assertEquals(0, names.getCount(5));
  }

  @Test
  public void whenNameIsSet_getNameReturnsName() {
    final var testName = "FB";

    final StorageEngineWriter storageEngineWriter = createPageTrxMock(testName);

    final var names = Names.getInstance(0);
    final var index = names.setName(testName, storageEngineWriter);
    final var name = names.getName(index);

    assertEquals(testName, name);
  }
}
