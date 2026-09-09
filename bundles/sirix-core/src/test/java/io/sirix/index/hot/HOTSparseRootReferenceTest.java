/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.CASPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.ValidTimeIndexPage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Sparse physical index IDs must resolve by offset, not by the number of populated references. */
final class HOTSparseRootReferenceTest {

  private static final int SPARSE_INDEX = 911;

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"PATH", "CAS", "NAME", "PROJECTION", "VALIDTIME"})
  void rootResolutionFindsSparseHighIdWithoutCreatingReferences(final IndexType indexType) {
    final StorageEngineReader storageEngineReader = mock(StorageEngineReader.class);
    final RevisionRootPage revisionRootPage = new RevisionRootPage();
    final PageReference expected = new PageReference().setKey(42L);
    final int referencesBefore;

    when(storageEngineReader.hasTrxIntentLog()).thenReturn(true);
    when(storageEngineReader.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    switch (indexType) {
      case PATH -> {
        final PathPage page = new PathPage();
        page.setOrCreateReference(SPARSE_INDEX, expected);
        referencesBefore = page.getReferencesCount();
        when(storageEngineReader.getPathPage(revisionRootPage)).thenReturn(page);
      }
      case CAS -> {
        final CASPage page = new CASPage();
        page.setOrCreateReference(SPARSE_INDEX, expected);
        referencesBefore = page.getReferencesCount();
        when(storageEngineReader.getCASPage(revisionRootPage)).thenReturn(page);
      }
      case NAME -> {
        final NamePage page = new NamePage();
        page.setOrCreateReference(SPARSE_INDEX, expected);
        referencesBefore = page.getReferencesCount();
        when(storageEngineReader.getNamePage(revisionRootPage)).thenReturn(page);
        doReturn(mock(JsonResourceSession.class)).when(storageEngineReader).getResourceSession();
      }
      case PROJECTION -> {
        final ProjectionIndexPage page = new ProjectionIndexPage();
        page.setOrCreateReference(SPARSE_INDEX, expected);
        referencesBefore = page.getReferencesCount();
        when(storageEngineReader.getProjectionIndexPage(revisionRootPage)).thenReturn(page);
      }
      case VALIDTIME -> {
        final ValidTimeIndexPage page = new ValidTimeIndexPage();
        page.setOrCreateReference(SPARSE_INDEX, expected);
        referencesBefore = page.getReferencesCount();
        when(storageEngineReader.getValidTimeIndexPage(revisionRootPage)).thenReturn(page);
      }
      default -> throw new AssertionError("Unexpected index type: " + indexType);
    }

    final RootProbe reader = new RootProbe(storageEngineReader, indexType, SPARSE_INDEX);
    assertTrue(referencesBefore < SPARSE_INDEX,
        "fixture must distinguish populated-reference count from physical slot offset");
    assertSame(expected, reader.getRootReference());
    assertEquals(referencesBefore, referencesCount(storageEngineReader, revisionRootPage, indexType));
  }

  private static int referencesCount(final StorageEngineReader storageEngineReader,
      final RevisionRootPage revisionRootPage, final IndexType indexType) {
    return switch (indexType) {
      case PATH -> storageEngineReader.getPathPage(revisionRootPage).getReferencesCount();
      case CAS -> storageEngineReader.getCASPage(revisionRootPage).getReferencesCount();
      case NAME -> storageEngineReader.getNamePage(revisionRootPage).getReferencesCount();
      case PROJECTION -> storageEngineReader.getProjectionIndexPage(revisionRootPage).getReferencesCount();
      case VALIDTIME -> storageEngineReader.getValidTimeIndexPage(revisionRootPage).getReferencesCount();
      default -> throw new AssertionError("Unexpected index type: " + indexType);
    };
  }

  private static final class RootProbe extends AbstractHOTIndexReader<Long> {

    private byte[] buffer = new byte[1];

    private RootProbe(final StorageEngineReader storageEngineReader, final IndexType indexType, final int indexNumber) {
      super(storageEngineReader, indexType, indexNumber);
    }

    @Override
    protected int serializeKey(final Long key, final byte[] buffer, final int offset) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected int maxSerializedKeyLength(final Long key) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected @Nullable Long deserializeKey(final byte[] buffer, final int offset, final int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected byte[] getKeyBuffer() {
      return buffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] newBuffer) {
      buffer = newBuffer;
    }

    @Override
    public Iterator<Map.Entry<Long, NodeReferences>> iterator() {
      return Collections.emptyIterator();
    }
  }
}
