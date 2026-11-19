package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.IOStorage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Public recursive-commit contracts: durable references, ownership release and failure before
 * write.
 */
final class PageReferenceCommitTest {
  @TempDir
  Path directory;

  private Database<JsonResourceSession> database;
  private JsonResourceSession session;
  private StorageEngineWriter writer;

  @BeforeEach
  void openWriter() {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("commit"))));
    database = Databases.openJsonDatabase(directory.resolve("commit"));
    database.createResource(ResourceConfiguration.newBuilder("r").storeDiffs(false).build());
    session = database.beginResourceSession("r");
    writer = session.createStorageEngineWriter();
  }

  @AfterEach
  void closeWriter() {
    try {
      writer.close();
    } finally {
      try {
        session.close();
      } finally {
        database.close();
        Databases.clearGlobalCaches();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, Integer.MAX_VALUE})
  void staleClaimWithoutAnyBackingPageCannotBecomeADanglingCommittedChild(final int logKey) {
    final PageReference stale = new PageReference();
    stale.setLogKey(logKey).setActiveTilGeneration(Integer.MAX_VALUE);
    assertThrows(SirixIOException.class, () -> writer.commit(stale));
    assertEquals(Constants.NULL_ID_LONG, stale.getKey());
  }

  @Test
  void pendingSidePageCannotBeSerializedBeforeItsBackgroundWriteCompletes() {
    final OverflowPage page = new OverflowPage(new byte[] {3, 1, 4});
    final PageReference reference = new PageReference();
    reference.setPage(page);
    reference.bindPendingPageWrite(page);
    try {
      assertThrows(SirixIOException.class, () -> writer.commit(reference));
      assertTrue(reference.hasPendingPageWrite());
      assertEquals(Constants.NULL_ID_LONG, reference.getKey());
      assertSame(page, reference.getPage());
    } finally {
      reference.cancelPendingPageWrite();
    }
  }

  @Test
  void unloggedSidePageGetsAResolvableOffsetAndReleasesItsSwizzle() {
    final PageReference reference = new PageReference();
    reference.setPage(new OverflowPage(new byte[] {3, 1, 4}));
    writer.commit(reference);
    assertTrue(reference.getKey() >= 0, "a parent must be able to serialize a resolvable child offset");
    assertNull(reference.getPage());
    final long durableKey = reference.getKey();
    writer.commit(reference);
    assertEquals(durableKey, reference.getKey(), "an already durable side page must not be rewritten");
  }

  @ParameterizedTest
  @CsvSource({"0,true", "43,true", "43,false"})
  void closedCowCopyInheritsItsOwnersOffsetAndHashPresence(final long key, final boolean hashed) {
    // Start a fresh log region so the valid zero log-key boundary is exercised too.
    writer.getLog().clear();
    final KeyValueLeafPage page = page(41);
    final PageReference original = new PageReference();
    writer.getLog().put(original, PageContainer.getInstance(page, page));
    assertEquals(0, original.getLogKey(), "fixture must exercise the valid zero log key");
    final PageReference copy = new PageReference(original);
    page.close();
    original.setKey(key);
    if (hashed) {
      original.setHash(0L); // A present zero hash is distinct from no hash.
    }
    copy.setKey(Constants.NULL_ID_LONG);
    copy.setHash(99L);
    writer.commit(copy);
    assertEquals(key, copy.getKey());
    assertEquals(hashed, copy.hasHash());
    assertEquals(0L, copy.getHashAsLong());
    assertTrue(page.isClosed());
  }

  @Test
  void liveCommitClosesBothOwnedImagesAndDetachesTheReference() {
    final KeyValueLeafPage complete = page(41);
    final KeyValueLeafPage modified = page(41);
    final PageReference reference = new PageReference();
    writer.getLog().put(reference, PageContainer.getInstance(complete, modified));
    reference.setPage(modified);
    assertFalse(complete.isClosed());
    assertFalse(modified.isClosed());
    writer.commit(reference);
    assertTrue(reference.getKey() >= 0);
    assertTrue(complete.isClosed());
    assertTrue(modified.isClosed());
    assertNull(reference.getPage());
  }

  @Test
  void spillFragmentCannotReachSerialization() {
    final KeyValueLeafPage page = page(41);
    final PageReference reference = new PageReference();
    writer.getLog().put(reference, PageContainer.getInstance(page, page));
    reference.addPageFragment(new PageFragmentKeyImpl(1, IOStorage.spillKey(0, 64), 0, 0));
    assertThrows(SirixIOException.class, () -> writer.commit(reference));
    assertEquals(Constants.NULL_ID_LONG, reference.getKey());
    assertFalse(page.isClosed(), "refusal must happen before serialization releases the page");
  }

  private KeyValueLeafPage page(final long key) {
    return new KeyValueLeafPage(key, IndexType.DOCUMENT, session.getResourceConfig(), writer.getRevisionNumber(), null,
        null, false);
  }
}
