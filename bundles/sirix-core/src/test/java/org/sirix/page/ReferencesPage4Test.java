package org.sirix.page;

import org.junit.jupiter.api.Test;
import org.sirix.page.delegates.ReferencesPage4;
import org.sirix.page.interfaces.PageFragmentKey;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the {@link ReferencesPage4}.
 *
 * @author Johannes Lichtenberger
 */
public final class ReferencesPage4Test {

  @Test
  public void testCloneConstructor() {
    final var referencesPage4 = new ReferencesPage4();

    final var pageReference = referencesPage4.getOrCreateReference(0);
    pageReference.setLogKey(5);

    final List<PageFragmentKey> pageFragmentKeys =
        List.of(new PageFragmentKeyImpl(1, 200), new PageFragmentKeyImpl(2, 763));

    pageReference.setPageFragments(pageFragmentKeys);

    final var newReferencesPage4 = new ReferencesPage4(referencesPage4);

    assertNotSame(referencesPage4, newReferencesPage4);

    final var copiedPageReference = newReferencesPage4.getOrCreateReference(0);

    assertEquals(pageReference.getLogKey(), copiedPageReference.getLogKey());

    final List<PageFragmentKey> copiedPageFragmentKeys = copiedPageReference.getPageFragments();

    assertEquals(copiedPageFragmentKeys.size(), 2);

    assertEquals(pageFragmentKeys.get(0).revision(), copiedPageFragmentKeys.get(0).revision());
    assertEquals(pageFragmentKeys.get(1).revision(), copiedPageFragmentKeys.get(1).revision());

    assertEquals(pageFragmentKeys.get(0).key(), copiedPageFragmentKeys.get(0).key());
    assertEquals(pageFragmentKeys.get(1).key(), copiedPageFragmentKeys.get(1).key());
  }
}
