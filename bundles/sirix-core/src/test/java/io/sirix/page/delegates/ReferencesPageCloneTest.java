package io.sirix.page.delegates;

import io.sirix.page.OverflowPage;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

/**
 * Regression tests for the clone constructors of {@link ReferencesPage4},
 * {@link BitmapReferencesPage} and {@link FullReferencesPage} (issue #1079).
 * <p>
 * Every reference must be routed through the {@link PageReference} copy constructor: a manual
 * field-by-field copy dropped the page-fragment hash ({@code hashInBytes}), aliased the fragment
 * list, and eagerly copied the swizzled in-memory page pointer (which can go stale and be read
 * after free through recycled frames).
 */
public final class ReferencesPageCloneTest {

  private static final long PERSISTENT_KEY = 123L;

  private static final byte[] HASH = {1, 2, 3, 4, 5};

  /**
   * Configure a source reference: a persistent storage key, a hash, a page-fragments list and a
   * swizzled in-memory page.
   *
   * @param reference the reference to configure
   */
  private static void configureSourceReference(final PageReference reference) {
    reference.setKey(PERSISTENT_KEY);
    reference.setHash(HASH.clone());
    final List<PageFragmentKey> pageFragments = new ArrayList<>(2);
    pageFragments.add(new PageFragmentKeyImpl(1, 42L, 7L, 8L));
    pageFragments.add(new PageFragmentKeyImpl(2, 43L, 7L, 8L));
    reference.setPageFragments(pageFragments);
    // Swizzled in-memory page; the reference is resolvable via its disk key, so a clone must
    // NOT alias this pointer.
    reference.setPage(new OverflowPage(new byte[] {9}));
  }

  /**
   * Assert the clone semantics of the {@link PageReference} copy constructor.
   *
   * @param source the configured source reference
   * @param clone the reference from the cloned page
   */
  private static void assertReferenceCloneSemantics(final PageReference source, final PageReference clone) {
    assertNotNull("cloned reference must exist", clone);
    assertNotSame("clone must be an independent PageReference instance", source, clone);
    assertEquals(PERSISTENT_KEY, clone.getKey());

    // (a) The hash bytes must be copied (pre-fix: null on the clone).
    assertArrayEquals("hash bytes must survive the clone", HASH, clone.getHash());

    // (b) The fragment list must be equal but must not alias the source list.
    assertEquals("page fragments must be equal", source.getPageFragments(), clone.getPageFragments());
    assertNotSame("page fragments must not alias the source list", source.getPageFragments(),
        clone.getPageFragments());

    // (c) The swizzled page pointer must NOT be copied when the reference is resolvable via a
    // persistent key (pre-fix: the raw pointer was copied and could be read after free).
    assertEquals(PERSISTENT_KEY, source.getKey());
    assertNotNull(source.getPage());
    assertNull("swizzled page pointer must not be aliased by the clone", clone.getPage());
  }

  @Test
  public void testReferencesPage4CloneConstructor() {
    final ReferencesPage4 source = new ReferencesPage4();
    final PageReference sourceReference = source.getOrCreateReference(0);
    assertNotNull(sourceReference);
    configureSourceReference(sourceReference);

    final ReferencesPage4 clone = new ReferencesPage4(source);
    final PageReference clonedReference = clone.getOrCreateReference(0);

    assertReferenceCloneSemantics(sourceReference, clonedReference);
  }

  @Test
  public void testBitmapReferencesPageCloneConstructor() {
    final BitmapReferencesPage source = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
    final PageReference sourceReference = source.getOrCreateReference(3);
    assertNotNull(sourceReference);
    configureSourceReference(sourceReference);

    final BitmapReferencesPage clone = new BitmapReferencesPage(source, source.getBitmap());
    final PageReference clonedReference = clone.getOrCreateReference(3);

    assertReferenceCloneSemantics(sourceReference, clonedReference);
  }

  @Test
  public void testFullReferencesPageCloneConstructor() {
    // A FullReferencesPage is only constructible from an existing page; an empty bitmap page
    // yields an all-null full page to which the source reference is added directly.
    final FullReferencesPage source =
        new FullReferencesPage(new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT));
    final PageReference sourceReference = source.getOrCreateReference(5);
    assertNotNull(sourceReference);
    configureSourceReference(sourceReference);

    final FullReferencesPage clone = new FullReferencesPage(source);
    final PageReference clonedReference = clone.getOrCreateReference(5);

    assertReferenceCloneSemantics(sourceReference, clonedReference);
  }
}
