/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.backup;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.io.StorageType;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TXN-615 clause 10 / AC14: a backup skips every IN_MEMORY resource and lists it as not backed up,
 * and a restore never creates one, while the persistent resources of the same database are backed
 * up and restored completely.
 */
final class BackupManagerInMemoryResourcesTest {

  enum Model {
    JSON, XML
  }

  private static final String VOLATILE = "volatile";

  private static final String DURABLE = "durable";

  @TempDir
  Path directory;

  @AfterEach
  void removeDatabases() {
    Databases.removeDatabase(directory.resolve("source"));
    Databases.removeDatabase(directory.resolve("restored"));
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void backupSkipsInMemoryResourcesAndRestoreNeverCreatesThem(final Model model) {
    final Path source = directory.resolve("source");
    final Path backup = directory.resolve("backup");
    final Path restored = directory.resolve("restored");
    if (model == Model.JSON) {
      Databases.createJsonDatabase(new DatabaseConfiguration(source));
    } else {
      Databases.createXmlDatabase(new DatabaseConfiguration(source));
    }
    final long[] durableKeys = new long[2];
    final BackupManager.BackupSummary backupSummary;
    try (Database<? extends ResourceSession<?, ?>> database = open(model, source)) {
      create(database, DURABLE, StorageType.FILE_CHANNEL);
      create(database, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> durable = database.beginResourceSession(DURABLE);
          ResourceSession<?, ?> volatileSession = database.beginResourceSession(VOLATILE)) {
        durableKeys[0] = commit(durable, model, -1L, "d0");
        durableKeys[1] = commit(durable, model, durableKeys[0], "d1");
        commit(volatileSession, model, -1L, "v0");
      }
      // An online backup while the IN_MEMORY resource holds committed state.
      backupSummary = BackupManager.backupDatabase(source, backup);
    }

    assertEquals(List.of(VOLATILE), backupSummary.notBackedUp(), "the IN_MEMORY resource is listed as not backed up");
    assertEquals(Set.of(DURABLE),
        backupSummary.resources()
                     .stream()
                     .map(BackupManager.ResourceSummary::resourceName)
                     .collect(Collectors.toSet()));

    final BackupManager.BackupSummary restoreSummary = BackupManager.restoreDatabase(backup, restored);
    assertEquals(Set.of(DURABLE),
        restoreSummary.resources()
                      .stream()
                      .map(BackupManager.ResourceSummary::resourceName)
                      .collect(Collectors.toSet()));
    try (Database<? extends ResourceSession<?, ?>> database = open(model, restored)) {
      assertEquals(Set.of(DURABLE),
          database.listResources().stream().map(path -> path.getFileName().toString()).collect(Collectors.toSet()));
      assertFalse(database.existsResource(VOLATILE), "a restore never creates an IN_MEMORY resource");
      try (ResourceSession<?, ?> durable = database.beginResourceSession(DURABLE)) {
        assertEquals(2, durable.getMostRecentRevisionNumber(), "the persistent resource is complete");
        assertEquals("d0", value(durable, model, 1, durableKeys[0]));
        assertEquals("d1", value(durable, model, 2, durableKeys[1]));
        assertNull(value(durable, model, 1, durableKeys[1]));
      }
    }
  }

  private static Database<? extends ResourceSession<?, ?>> open(final Model model, final Path path) {
    return model == Model.JSON
        ? Databases.openJsonDatabase(path)
        : Databases.openXmlDatabase(path);
  }

  private static void create(final Database<? extends ResourceSession<?, ?>> database, final String name,
      final StorageType type) {
    assertTrue(database.createResource(ResourceConfiguration.newBuilder(name)
                                                            .storageType(type)
                                                            .hashKind(HashType.NONE)
                                                            .buildPathSummary(false)
                                                            .build()));
  }

  /** Commit one value: the first one in a new container when {@code previousKey} is negative. */
  private static long commit(final ResourceSession<?, ?> session, final Model model, final long previousKey,
      final String value) {
    try (NodeTrx writer = session.beginNodeTrx()) {
      if (previousKey < 0) {
        if (model == Model.JSON) {
          ((JsonNodeTrx) writer).insertArrayAsFirstChild();
          ((JsonNodeTrx) writer).insertStringValueAsFirstChild(value);
        } else {
          final XmlNodeTrx xml = (XmlNodeTrx) writer;
          xml.insertElementAsFirstChild(new QNm("root"));
          xml.insertElementAsFirstChild(new QNm("item"));
          xml.insertTextAsFirstChild(value);
        }
      } else {
        assertTrue(writer.moveTo(previousKey));
        if (model == Model.JSON) {
          ((JsonNodeTrx) writer).insertStringValueAsRightSibling(value);
        } else {
          final XmlNodeTrx xml = (XmlNodeTrx) writer;
          xml.moveToParent();
          xml.insertElementAsRightSibling(new QNm("item"));
          xml.insertTextAsFirstChild(value);
        }
      }
      final long key = writer.getNodeKey();
      writer.commit();
      return key;
    }
  }

  private static @Nullable String value(final ResourceSession<?, ?> session, final Model model, final int revision,
      final long key) {
    try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      if (!reader.moveTo(key)) {
        return null;
      }
      return model == Model.JSON
          ? ((JsonNodeReadOnlyTrx) reader).getValue()
          : ((XmlNodeReadOnlyTrx) reader).getValue();
    }
  }
}
