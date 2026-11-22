/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.ram;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.Writer;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code RAMStorage.truncateTo} must not report a rollback it cannot perform.
 *
 * <p>
 * Its callers — crash recovery in {@code AbstractResourceSession} and explicit rollback via
 * {@code NodeStorageEngineWriter.truncateTo} — treat a normal return as "the resource is now at
 * that revision", and go on to commit on top of that assumption. This storage keeps pages in a flat
 * {@code pageKey -> Page} map with no record of which revision wrote them, and retains only the
 * current uber-page pointer, so it can identify neither the pages to discard nor the uber page to
 * restore. The method was an empty {@code // TODO} that returned successfully.
 * </p>
 */
@Isolated
@Order(Integer.MAX_VALUE)
public final class RAMStorageTruncateToTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void publishedRootReferenceReadsTheLatestRootByItsStorageKey() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final ResourceConfiguration config;
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      config = session.getResourceConfig();
    }
    final RAMStorage storage = new RAMStorage(config);
    try (final Writer writer = storage.createWriter()) {
      final BytesOut<?> buffer = mock(BytesOut.class);
      writer.write(config, new PageReference(), new UberPage(1), buffer);
      final UberPage published = new UberPage(2);
      final PageReference written = new PageReference();
      writer.writeUberPageReference(config, written, published, buffer);
      final PageReference reference = writer.readUberPageReference();
      assertEquals(written.getKey(), reference.getKey());
      assertSame(published, reference.getPage());
      assertSame(published, writer.read(reference, config));
    } finally {
      storage.close();
    }
  }

  @Test
  void truncateToRefusesRatherThanSilentlyDoingNothing() {
    // A fully-wired ResourceConfiguration: RAMStorage resolves its per-resource maps through the
    // configured resource path, which only a real resource has.
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final ResourceConfiguration resourceConfig;
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      resourceConfig = session.getResourceConfig();
    }

    final RAMStorage storage = new RAMStorage(resourceConfig);
    final Writer writer = storage.createWriter();

    // The capability query is what callers actually branch on — they ask BEFORE mutating anything,
    // because truncateTo is the last step of a sequence that has already downgraded the uber-page
    // beacons and the session's last-committed uber page. Pinned in the same test as the throw so
    // the two cannot drift: making truncateTo work without flipping this leaves callers refusing a
    // rollback the storage could now perform, and flipping this without making it work restores the
    // half-applied state the capability was introduced to prevent.
    assertFalse(writer.supportsTruncateTo(),
        "a storage that cannot truncate must SAY so, not just throw once a rollback is under way");

    final UnsupportedOperationException failure = assertThrows(UnsupportedOperationException.class,
        () -> writer.truncateTo(1), "a rollback this storage cannot honour must fail, not return as if it succeeded");
    assertTrue(failure.getMessage().contains("cannot truncate"),
        "the message should state what is unsupported; got: " + failure.getMessage());

    storage.close();
  }
}
