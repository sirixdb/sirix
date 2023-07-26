package io.sirix.index.name;

import io.sirix.api.PageTrx;
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
    final PageTrx pageTrx = createPageTrxMock("FB");

    final var names = Names.getInstance(0);

    final var fbIndex = names.setName("FB", pageTrx);
    final var EaIndex = names.setName("Ea", pageTrx);

    assertNotEquals(fbIndex, EaIndex);
  }

  private PageTrx createPageTrxMock(String name) {
    final var hashEntryNode = new HashEntryNode(2, 12, name);
    final var hashCountEntryNode = new HashCountEntryNode(3, 1);

    @SuppressWarnings("unchecked")
    final PageTrx pageTrx = mock(PageTrx.class);
    when(pageTrx.createRecord(any(HashEntryNode.class), eq(IndexType.NAME), eq(0))).thenReturn(
        hashEntryNode);
    when(pageTrx.createRecord(any(HashCountEntryNode.class), eq(IndexType.NAME), eq(0))).thenReturn(
        hashCountEntryNode);
    when(pageTrx.prepareRecordForModification(2L, IndexType.NAME, 0)).thenReturn(hashCountEntryNode);
    return pageTrx;
  }

  @Test
  public void whenIndexExistsForSameString_createNoNewIndex() {
    final PageTrx pageTrx = createPageTrxMock("FB");

    final var names = Names.getInstance(0);

    final var fbIndex = names.setName("FB", pageTrx);
    final var EaIndex = names.setName("FB", pageTrx);

    assertEquals(fbIndex, EaIndex);
  }

  @Test
  public void whenIndexExistsForSameString_increaseCounter() {
    final PageTrx pageTrx = createPageTrxMock("FB");

    final var names = Names.getInstance(0);

    final var index = names.setName("FB", pageTrx);
    names.setName("FB", pageTrx);

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

    final PageTrx pageTrx = createPageTrxMock(testName);

    final var names = Names.getInstance(0);
    final var index = names.setName(testName, pageTrx);
    final var name = names.getName(index);

    assertEquals(testName, name);
  }
}
