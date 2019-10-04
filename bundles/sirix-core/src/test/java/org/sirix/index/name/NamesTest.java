package org.sirix.index.name;

import org.junit.Test;
import org.sirix.api.PageTrx;
import org.sirix.node.HashCountEntryNode;
import org.sirix.node.HashEntryNode;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.UnorderedKeyValuePage;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotEquals;

public final class NamesTest {
  @Test
  public void whenIndexExistsForAnotherString_createNewIndex() {
    final PageTrx<Long, Record, UnorderedKeyValuePage> pageTrx = createPageTrxMock("FB");

    final var names = Names.getInstance(0);

    final var fbIndex = names.setName("FB", pageTrx);
    final var EaIndex = names.setName("Ea", pageTrx);

    assertNotEquals(fbIndex, EaIndex);
  }

  private PageTrx<Long, Record, UnorderedKeyValuePage> createPageTrxMock(String name) {
    final var hashEntryNode = new HashEntryNode(2, 12, name);
    final var hashCountEntryNode = new HashCountEntryNode(3, 1);

    @SuppressWarnings("unchecked")
    final PageTrx<Long, Record, UnorderedKeyValuePage> pageTrx = mock(PageTrx.class);
    when(pageTrx.createEntry(anyLong(), any(HashEntryNode.class), eq(PageKind.NAMEPAGE), eq(0))).thenReturn(
        hashEntryNode);
    when(pageTrx.createEntry(anyLong(), any(HashCountEntryNode.class), eq(PageKind.NAMEPAGE), eq(0))).thenReturn(
        hashCountEntryNode);
    when(pageTrx.prepareEntryForModification(2L, PageKind.NAMEPAGE, 0)).thenReturn(hashCountEntryNode);
    return pageTrx;
  }

  @Test
  public void whenIndexExistsForSameString_createNoNewIndex() {
    final PageTrx<Long, Record, UnorderedKeyValuePage> pageTrx = createPageTrxMock("FB");

    final var names = Names.getInstance(0);

    final var fbIndex = names.setName("FB", pageTrx);
    final var EaIndex = names.setName("FB", pageTrx);

    assertEquals(fbIndex, EaIndex);
  }

  @Test
  public void whenIndexExistsForSameString_increaseCounter() {
    final PageTrx<Long, Record, UnorderedKeyValuePage> pageTrx = createPageTrxMock("FB");

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

    final PageTrx<Long, Record, UnorderedKeyValuePage> pageTrx = createPageTrxMock(testName);

    final var names = Names.getInstance(0);

    final var index = names.setName(testName, pageTrx);

    final var name = names.getName(index);

    assertEquals(testName, name);
  }
}
