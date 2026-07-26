/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.ram;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.Writer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code RAMStorage.truncateTo} must not report a rollback it cannot perform.
 *
 * <p>Its callers — crash recovery in {@code AbstractResourceSession} and explicit rollback via
 * {@code NodeStorageEngineWriter.truncateTo} — treat a normal return as "the resource is now at that
 * revision", and go on to commit on top of that assumption. This storage keeps pages in a flat
 * {@code pageKey -> Page} map with no record of which revision wrote them, and retains only the
 * current uber-page pointer, so it can identify neither the pages to discard nor the uber page to
 * restore. The method was an empty {@code // TODO} that returned successfully.</p>
 */
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

    final UnsupportedOperationException failure =
        assertThrows(UnsupportedOperationException.class, () -> writer.truncateTo(1),
            "a rollback this storage cannot honour must fail, not return as if it succeeded");
    assertTrue(failure.getMessage().contains("cannot truncate"),
        "the message should state what is unsupported; got: " + failure.getMessage());

    storage.close();
  }
}
