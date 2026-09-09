/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.page.CASPage;
import io.sirix.page.NamePage;
import io.sirix.page.PathPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.ValidTimeIndexPage;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Lifecycle-boundary coverage for indexes whose payload is not document-derived. */
final class IndexLifecycleGuardTest {

  private static final int LOGICAL_ID = 7;

  @ParameterizedTest(name = "initialized {0} physical id is rejected before creation")
  @EnumSource(value = IndexType.class, names = {"PATH", "CAS", "NAME", "PROJECTION", "VALIDTIME"})
  void initializedPhysicalIdWithoutCatalogDefinitionFailsBeforeMutation(final IndexType indexType) {
    final JsonIndexController controller = new JsonIndexController();
    final IndexDef indexDef = definition(indexType);
    final JsonNodeTrx wtx = mock(JsonNodeTrx.class);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    when(wtx.getStorageEngineWriter()).thenReturn(storageEngineWriter);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    if (indexType == IndexType.VALIDTIME) {
      final JsonResourceSession resourceSession = mock(JsonResourceSession.class);
      final ResourceConfiguration resourceConfiguration =
          ResourceConfiguration.newBuilder("guard-resource").validTimePaths("validFrom", "validTo").build();
      when(wtx.getResourceSession()).thenReturn(resourceSession);
      when(resourceSession.getResourceConfig()).thenReturn(resourceConfiguration);
    }
    reservePhysicalId(indexType, indexDef, storageEngineWriter, revisionRootPage);

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> controller.createIndexes(Set.of(indexDef), wtx));

    assertTrue(failure.getMessage().contains(indexType + " index " + indexDef.getID()));
    assertTrue(controller.getIndexes().getIndexDefs().isEmpty(), "guard failure catalogued the rejected definition");
    if (indexType == IndexType.VALIDTIME) {
      verify(wtx).getResourceSession();
    }
    verify(wtx).getStorageEngineWriter();
    verifyNoMoreInteractions(wtx);
    verifyReadOnlyPhysicalProbe(indexType, storageEngineWriter, revisionRootPage);
  }

  @Test
  void loadTimeProjectionArmRejectsInitializedPhysicalIdBeforePublishingOwnerOrCatalog() throws PathException {
    final JsonIndexController controller = new JsonIndexController();
    final IndexDef projection = definition(IndexType.PROJECTION);
    final JsonNodeTrx wtx = mock(JsonNodeTrx.class);
    final JsonResourceSession resourceSession = mock(JsonResourceSession.class);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    final PathSummaryReader pathSummary = mock(PathSummaryReader.class);
    final ResourceConfiguration resourceConfiguration =
        ResourceConfiguration.newBuilder("guard-resource").buildPathSummary(true).build();
    when(wtx.getResourceSession()).thenReturn(resourceSession);
    when(resourceSession.getResourceConfig()).thenReturn(resourceConfiguration);
    when(wtx.getPathSummary()).thenReturn(pathSummary);
    when(pathSummary.getPCRsForPaths(Set.of(projection.getProjectionRootPath()))).thenReturn(new LongOpenHashSet());
    when(wtx.getStorageEngineWriter()).thenReturn(storageEngineWriter);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    reservePhysicalId(IndexType.PROJECTION, projection, storageEngineWriter, revisionRootPage);

    final IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> controller.createProjectionIndexAtLoadStart(projection, wtx, 1_000L));

    assertTrue(failure.getMessage().contains("PROJECTION index " + projection.getID()));
    assertTrue(controller.getIndexes().getIndexDefs().isEmpty(), "load-time guard published the rejected definition");
    verify(wtx, never()).awaitPendingAsyncCommit();
    verifyReadOnlyPhysicalProbe(IndexType.PROJECTION, storageEngineWriter, revisionRootPage);
  }

  @Test
  void xmlNameGuardUsesTheXmlSecondarySlotRange() {
    final XmlIndexController controller = new XmlIndexController();
    final IndexDef indexDef = IndexDefs.createNameIdxDef(LOGICAL_ID, IndexDef.DbType.XML);
    final XmlNodeTrx wtx = mock(XmlNodeTrx.class);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    final NamePage namePage = new NamePage();
    namePage.incrementAndGetMaxHotPageKey(indexDef.getID());
    when(wtx.getStorageEngineWriter()).thenReturn(storageEngineWriter);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(storageEngineWriter.getNamePage(revisionRootPage)).thenReturn(namePage);
    doReturn(mock(XmlResourceSession.class)).when(storageEngineWriter).getResourceSession();

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> controller.createIndexes(Set.of(indexDef), wtx));

    assertTrue(failure.getMessage().contains("NAME index " + indexDef.getID()));
    assertTrue(controller.getIndexes().getIndexDefs().isEmpty());
    verify(wtx).getStorageEngineWriter();
    verifyNoMoreInteractions(wtx);
    verifyReadOnlyPhysicalProbe(IndexType.NAME, storageEngineWriter, revisionRootPage);
  }

  @Test
  void jsonStandardCreationRejectsVectorBeforeCataloguingAnyDefinition() {
    final JsonIndexController controller = new JsonIndexController();
    final IndexDef vector = vectorDefinition(IndexDef.DbType.JSON);
    final IndexDef path = IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON);

    assertThrows(UnsupportedOperationException.class,
        () -> controller.createIndexes(Set.of(path, vector), mock(JsonNodeTrx.class)));

    assertTrue(controller.getIndexes().getIndexDefs().isEmpty());
  }

  @Test
  void xmlStandardCreationRejectsVectorBeforeCataloguingIt() {
    final XmlIndexController controller = new XmlIndexController();
    final IndexDef vector = vectorDefinition(IndexDef.DbType.XML);

    assertThrows(UnsupportedOperationException.class,
        () -> controller.createIndexes(Set.of(vector), mock(XmlNodeTrx.class)));

    assertTrue(controller.getIndexes().getIndexDefs().isEmpty());
  }

  @Test
  void listenerRegistrationCannotBeUsedToCatalogNewVectorDefinition() {
    final JsonIndexController controller = new JsonIndexController();
    final IndexDef vector = vectorDefinition(IndexDef.DbType.JSON);

    assertThrows(UnsupportedOperationException.class,
        () -> controller.createIndexListeners(Set.of(vector), mock(JsonNodeTrx.class)));

    assertNull(controller.getIndexes().getIndexDef(vector.getID(), IndexType.VECTOR));
  }

  @Test
  void persistedExplicitVectorDefinitionCanRebindForExplicitApiCompatibility() {
    final JsonIndexController controller = new JsonIndexController();
    final IndexDef vector = vectorDefinition(IndexDef.DbType.JSON);
    controller.getIndexes().add(vector);

    assertDoesNotThrow(() -> controller.createIndexListeners(Set.of(vector), mock(JsonNodeTrx.class)));

    assertTrue(controller.hasVectorIndex());
  }

  @Test
  void samePhysicalIdCannotBeReboundToDifferentDefinition() {
    final JsonIndexController controller = new JsonIndexController();
    final IndexDef existing = IndexDefs.createPathIdxDef(Set.of(Path.parse("/[]/old", PathParser.Type.JSON)),
        LOGICAL_ID, IndexDef.DbType.JSON);
    final IndexDef requested = IndexDefs.createPathIdxDef(Set.of(Path.parse("/[]/new", PathParser.Type.JSON)),
        LOGICAL_ID, IndexDef.DbType.JSON);
    controller.getIndexes().add(existing);
    final JsonNodeTrx wtx = mock(JsonNodeTrx.class);

    final IllegalStateException creationFailure =
        assertThrows(IllegalStateException.class, () -> controller.createIndexes(Set.of(requested), wtx));
    assertTrue(creationFailure.getMessage().contains("different definition"));
    assertSame(existing, controller.getIndexes().getIndexDef(LOGICAL_ID, IndexType.PATH),
        "the rejected definition replaced the catalogued definition");
    assertEquals(1, controller.getIndexes().getIndexDefs().size());
    verifyNoMoreInteractions(wtx);

    final IllegalStateException listenerFailure =
        assertThrows(IllegalStateException.class, () -> controller.createIndexListeners(Set.of(requested), wtx));
    assertTrue(listenerFailure.getMessage().contains("different definition"));
    verifyNoMoreInteractions(wtx);
  }

  @Test
  void xmlValidTimeDefinitionIsRejectedBeforeCatalogOrTransactionMutation() {
    final XmlIndexController controller = new XmlIndexController();
    final IndexDef validTime = IndexDefs.createValidTimeIdxDef(Set.of(Path.parse("/validFrom", PathParser.Type.XML)),
        LOGICAL_ID, IndexDef.DbType.XML);
    final XmlNodeTrx wtx = mock(XmlNodeTrx.class);

    assertThrows(UnsupportedOperationException.class, () -> controller.createIndexes(Set.of(validTime), wtx));
    assertTrue(controller.getIndexes().getIndexDefs().isEmpty());
    verifyNoMoreInteractions(wtx);

    assertThrows(UnsupportedOperationException.class, () -> controller.createIndexListeners(Set.of(validTime), wtx));
    assertTrue(controller.getIndexes().getIndexDefs().isEmpty());
    verifyNoMoreInteractions(wtx);
  }

  @Test
  void jsonValidTimeDefinitionWithoutResourceConfigIsRejectedBeforeCatalogOrPhysicalMutation() {
    final JsonIndexController controller = new JsonIndexController();
    final IndexDef validTime = definition(IndexType.VALIDTIME);
    final JsonNodeTrx wtx = mock(JsonNodeTrx.class);
    final JsonResourceSession resourceSession = mock(JsonResourceSession.class);
    final ResourceConfiguration resourceConfiguration = ResourceConfiguration.newBuilder("guard-resource").build();
    when(wtx.getResourceSession()).thenReturn(resourceSession);
    when(resourceSession.getResourceConfig()).thenReturn(resourceConfiguration);

    final IllegalStateException creationFailure =
        assertThrows(IllegalStateException.class, () -> controller.createIndexes(Set.of(validTime), wtx));
    assertTrue(creationFailure.getMessage().contains("no ValidTimeConfig"));
    assertTrue(controller.getIndexes().getIndexDefs().isEmpty());
    verify(wtx).getResourceSession();
    verifyNoMoreInteractions(wtx);

    final IllegalStateException listenerFailure =
        assertThrows(IllegalStateException.class, () -> controller.createIndexListeners(Set.of(validTime), wtx));
    assertTrue(listenerFailure.getMessage().contains("no ValidTimeConfig"));
    assertTrue(controller.getIndexes().getIndexDefs().isEmpty());
  }

  private static IndexDef vectorDefinition(final IndexDef.DbType dbType) {
    return IndexDefs.createVectorIdxDef(4, "L2", Set.of(), 0, dbType);
  }

  private static IndexDef definition(final IndexType indexType) {
    final Path<QNm> fieldPath = Path.parse("/[]/value", PathParser.Type.JSON);
    return switch (indexType) {
      case PATH -> IndexDefs.createPathIdxDef(Set.of(fieldPath), LOGICAL_ID, IndexDef.DbType.JSON);
      case CAS -> IndexDefs.createCASIdxDef(false, Type.STR, Set.of(fieldPath), LOGICAL_ID, IndexDef.DbType.JSON);
      case NAME -> IndexDefs.createNameIdxDef(LOGICAL_ID, IndexDef.DbType.JSON);
      case PROJECTION -> IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), List.of(fieldPath),
          List.of(Type.STR), LOGICAL_ID, IndexDef.DbType.JSON);
      case VALIDTIME -> IndexDefs.createValidTimeIdxDef(Set.of(fieldPath), LOGICAL_ID, IndexDef.DbType.JSON);
      default -> throw new IllegalArgumentException("unsupported guard fixture type: " + indexType);
    };
  }

  private static void reservePhysicalId(final IndexType indexType, final IndexDef indexDef,
      final StorageEngineWriter storageEngineWriter, final RevisionRootPage revisionRootPage) {
    switch (indexType) {
      case PATH -> {
        final PathPage page = new PathPage();
        page.incrementAndGetMaxHotPageKey(indexDef.getID());
        when(storageEngineWriter.getPathPage(revisionRootPage)).thenReturn(page);
      }
      case CAS -> {
        final CASPage page = new CASPage();
        page.incrementAndGetMaxHotPageKey(indexDef.getID());
        when(storageEngineWriter.getCASPage(revisionRootPage)).thenReturn(page);
      }
      case NAME -> {
        final NamePage page = new NamePage();
        page.incrementAndGetMaxHotPageKey(indexDef.getID());
        when(storageEngineWriter.getNamePage(revisionRootPage)).thenReturn(page);
        doReturn(mock(JsonResourceSession.class)).when(storageEngineWriter).getResourceSession();
      }
      case PROJECTION -> {
        final ProjectionIndexPage page = new ProjectionIndexPage();
        page.incrementAndGetMaxHotPageKey(indexDef.getID());
        when(storageEngineWriter.getProjectionIndexPage(revisionRootPage)).thenReturn(page);
      }
      case VALIDTIME -> {
        final ValidTimeIndexPage page = new ValidTimeIndexPage();
        page.incrementAndGetMaxHotPageKey(indexDef.getID());
        when(storageEngineWriter.getValidTimeIndexPage(revisionRootPage)).thenReturn(page);
      }
      default -> throw new IllegalArgumentException("unsupported guard fixture type: " + indexType);
    }
  }

  private static void verifyReadOnlyPhysicalProbe(final IndexType indexType,
      final StorageEngineWriter storageEngineWriter, final RevisionRootPage revisionRootPage) {
    verify(storageEngineWriter).getActualRevisionRootPage();
    switch (indexType) {
      case PATH -> verify(storageEngineWriter).getPathPage(revisionRootPage);
      case CAS -> verify(storageEngineWriter).getCASPage(revisionRootPage);
      case NAME -> {
        verify(storageEngineWriter).getNamePage(revisionRootPage);
        verify(storageEngineWriter).getResourceSession();
      }
      case PROJECTION -> verify(storageEngineWriter).getProjectionIndexPage(revisionRootPage);
      case VALIDTIME -> verify(storageEngineWriter).getValidTimeIndexPage(revisionRootPage);
      default -> throw new IllegalArgumentException("unsupported guard fixture type: " + indexType);
    }
    verifyNoMoreInteractions(storageEngineWriter);
  }
}
