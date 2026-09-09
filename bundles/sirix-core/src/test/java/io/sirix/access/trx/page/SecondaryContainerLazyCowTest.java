/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.index.IndexType;
import io.sirix.page.CASPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.ValidTimeIndexPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Correctness gates for the one lazy first-touch CoW route shared by every HOT container page. */
final class SecondaryContainerLazyCowTest {

  private static final int INDEX_NUMBER = 7;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void firstTouchIsLazyPrivateAndVersioned(final VersioningType versioningType) {
    final String resource = "secondary-container-cow-" + versioningType.name().toLowerCase();
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(versioningType)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(resource)) {
        // Finish the bootstrap revision. Its freshly created NamePage is intentionally private;
        // laziness is a requirement for subsequent revisions backed by persisted history.
        try (JsonNodeTrx bootstrap = session.beginNodeTrx()) {
          bootstrap.insertObjectAsFirstChild();
          bootstrap.commit();
        }
        final int baselineRevision = session.getMostRecentRevisionNumber();

        try (JsonNodeTrx emptyWtx = session.beginNodeTrx()) {
          assertNoSecondaryContainerInLog((NodeStorageEngineWriter) emptyWtx.getStorageEngineWriter());
          emptyWtx.commit();
        }
        final int emptyRevision = session.getMostRecentRevisionNumber();
        assertRevisionCounters(session, baselineRevision, 0L);
        assertRevisionCounters(session, emptyRevision, 0L);

        final Page[] historicalPages = new Page[IndexType.values().length];
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
          assertNoSecondaryContainerInLog(writer);

          for (final IndexType type : secondaryIndexTypes()) {
            final Page historicalPage =
                readContainer(writer.getStorageEngineReader(), writer.getActualRevisionRootPage(), type);
            historicalPages[type.ordinal()] = historicalPage;
            assertEquals(0L, maxHotPageKey(historicalPage, type));

            final int entriesBeforeFirstTouch = writer.getLog().liveEntryCount();
            final Page privatePage = writer.prepareSecondaryIndexPage(type);
            assertEquals(entriesBeforeFirstTouch + 1, writer.getLog().liveEntryCount(),
                type + " first touch must publish exactly one container");
            assertNotSame(historicalPage, privatePage, type + " must detach before first mutation");
            assertSame(privatePage, writer.prepareSecondaryIndexPage(type),
                type + " repeat touch must reuse the one private page");
            assertEquals(entriesBeforeFirstTouch + 1, writer.getLog().liveEntryCount(),
                type + " repeat touch must not publish another container");
            assertSame(privatePage, readContainer(writer, writer.getActualRevisionRootPage(), type),
                type + " writer getter must resolve the private modified page");

            final PageContainer container =
                writer.getLog().get(containerReference(writer.getActualRevisionRootPage(), type));
            assertSame(privatePage, container.getComplete(), type + " complete half must be transaction-private");
            assertSame(privatePage, container.getModified(), type + " modified half must be the same private page");

            assertEquals(1L, incrementAndGetMaxHotPageKey(privatePage, type));
            assertEquals(0L, maxHotPageKey(historicalPage, type),
                type + " allocator mutation leaked into the cached historical page");
          }
          wtx.commit();
        }
        final int firstMutationRevision = session.getMostRecentRevisionNumber();

        assertRevisionCounters(session, baselineRevision, 0L);
        assertRevisionCounters(session, firstMutationRevision, 1L);

        // Exercise the two late-added containers through a second CoW boundary. These used to be
        // split between an eager factory path and projection-only/valid-time-only ad-hoc clones.
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
          assertNoSecondaryContainerInLog(writer);
          for (final IndexType type : new IndexType[] {IndexType.PROJECTION, IndexType.VALIDTIME}) {
            final Page historicalPage =
                readContainer(writer.getStorageEngineReader(), writer.getActualRevisionRootPage(), type);
            final Page privatePage = writer.prepareSecondaryIndexPage(type);
            assertNotSame(historicalPage, privatePage);
            assertEquals(2L, incrementAndGetMaxHotPageKey(privatePage, type));
            assertEquals(1L, maxHotPageKey(historicalPage, type));
          }
          wtx.commit();
        }
        final int secondMutationRevision = session.getMostRecentRevisionNumber();

        assertRevisionCounter(session, firstMutationRevision, IndexType.PROJECTION, 1L);
        assertRevisionCounter(session, firstMutationRevision, IndexType.VALIDTIME, 1L);
        assertRevisionCounter(session, secondMutationRevision, IndexType.PROJECTION, 2L);
        assertRevisionCounter(session, secondMutationRevision, IndexType.VALIDTIME, 2L);

        for (final IndexType type : secondaryIndexTypes()) {
          assertEquals(0L, maxHotPageKey(historicalPages[type.ordinal()], type),
              type + " retained historical object changed after later commits");
        }
      }
    }
  }

  private static void assertNoSecondaryContainerInLog(final NodeStorageEngineWriter writer) {
    for (final PageContainer container : writer.getLog().getList()) {
      assertFalse(isSecondaryContainer(container.getComplete()));
      assertFalse(isSecondaryContainer(container.getModified()));
    }
  }

  private static boolean isSecondaryContainer(final Page page) {
    return page instanceof PathPage || page instanceof CASPage || page instanceof NamePage
        || page instanceof ProjectionIndexPage || page instanceof ValidTimeIndexPage;
  }

  private static IndexType[] secondaryIndexTypes() {
    return new IndexType[] {IndexType.PATH, IndexType.CAS, IndexType.NAME, IndexType.PROJECTION, IndexType.VALIDTIME};
  }

  private static void assertRevisionCounters(final JsonResourceSession session, final int revision,
      final long expected) {
    for (final IndexType type : secondaryIndexTypes()) {
      assertRevisionCounter(session, revision, type, expected);
    }
  }

  private static void assertRevisionCounter(final JsonResourceSession session, final int revision, final IndexType type,
      final long expected) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final Page page = readContainer(reader, reader.getActualRevisionRootPage(), type);
      assertEquals(expected, maxHotPageKey(page, type), type + " at revision " + revision);
    }
  }

  private static Page readContainer(final StorageEngineReader reader, final RevisionRootPage revisionRoot,
      final IndexType type) {
    return switch (type) {
      case PATH -> reader.getPathPage(revisionRoot);
      case CAS -> reader.getCASPage(revisionRoot);
      case NAME -> reader.getNamePage(revisionRoot);
      case PROJECTION -> reader.getProjectionIndexPage(revisionRoot);
      case VALIDTIME -> reader.getValidTimeIndexPage(revisionRoot);
      default -> throw new IllegalArgumentException("Not a secondary index type: " + type);
    };
  }

  private static PageReference containerReference(final RevisionRootPage revisionRoot, final IndexType type) {
    return switch (type) {
      case PATH -> revisionRoot.getPathPageReference();
      case CAS -> revisionRoot.getCASPageReference();
      case NAME -> revisionRoot.getNamePageReference();
      case PROJECTION -> revisionRoot.getProjectionIndexPageReference();
      case VALIDTIME -> revisionRoot.getValidTimeIndexPageReference();
      default -> throw new IllegalArgumentException("Not a secondary index type: " + type);
    };
  }

  private static long maxHotPageKey(final Page page, final IndexType type) {
    return switch (type) {
      case PATH -> ((PathPage) page).getMaxHotPageKey(INDEX_NUMBER);
      case CAS -> ((CASPage) page).getMaxHotPageKey(INDEX_NUMBER);
      case NAME -> ((NamePage) page).getMaxHotPageKey(INDEX_NUMBER);
      case PROJECTION -> ((ProjectionIndexPage) page).getMaxHotPageKey(INDEX_NUMBER);
      case VALIDTIME -> ((ValidTimeIndexPage) page).getMaxHotPageKey(INDEX_NUMBER);
      default -> throw new IllegalArgumentException("Not a secondary index type: " + type);
    };
  }

  private static long incrementAndGetMaxHotPageKey(final Page page, final IndexType type) {
    return switch (type) {
      case PATH -> ((PathPage) page).incrementAndGetMaxHotPageKey(INDEX_NUMBER);
      case CAS -> ((CASPage) page).incrementAndGetMaxHotPageKey(INDEX_NUMBER);
      case NAME -> ((NamePage) page).incrementAndGetMaxHotPageKey(INDEX_NUMBER);
      case PROJECTION -> ((ProjectionIndexPage) page).incrementAndGetMaxHotPageKey(INDEX_NUMBER);
      case VALIDTIME -> ((ValidTimeIndexPage) page).incrementAndGetMaxHotPageKey(INDEX_NUMBER);
      default -> throw new IllegalArgumentException("Not a secondary index type: " + type);
    };
  }
}
