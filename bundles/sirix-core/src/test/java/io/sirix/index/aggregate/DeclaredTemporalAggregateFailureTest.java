package io.sirix.index.aggregate;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceAccessScope;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Declaring a temporal aggregate installs a hidden derived resource. The declaration is the
 * request, so a derived resource that cannot be installed must fail the declaration instead of
 * leaving the resource silently without the index it was asked to create.
 *
 * <p>
 * The failure is injected through the production authorization seam
 * {@link ResourceAccessScope}: a policy that may create {@code records} but not the hidden
 * {@code records$derived} — the shape a REST role without CREATE on the derived namespace has.
 * That makes the failure deterministic, with no timing involved.
 */
final class DeclaredTemporalAggregateFailureTest {

  @TempDir
  private File directory;

  /** A policy that grants everything except creating a derived resource. */
  private static final class DenyDerivedCreation implements ResourceAccessScope.Policy {
    @Override
    public void checkDatabase(final java.nio.file.Path database, final boolean write) {
      // Every database access is granted; only the derived resource is withheld below.
    }

    @Override
    public void checkResource(final java.nio.file.Path database, final String resource, final boolean write) {
      if (write && DerivedIndexMaintainer.isDerivedResourceName(resource)) {
        throw new SirixUsageException("No permission to create " + resource);
      }
    }

    @Override
    public void checkWrite() {
      // Writing the source resource is granted.
    }
  }

  @Test
  void aDeclarationWhoseDerivedResourceCannotBeInstalledFails() {
    final var path = directory.toPath().resolve("denied");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(newRecordsResource());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final IndexDef projection = declareProjection(session, writer);

        final SirixUsageException denied = assertThrows(SirixUsageException.class,
            () -> ResourceAccessScope.where(new DenyDerivedCreation())
                                     .run(() -> TemporalAggregateDeclarations.declare(
                                         session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
                                         List.of("g"), List.of("sum(n)"))),
            "A declaration that cannot install its derived resource must not report success");
        assertTrue(denied.getMessage().contains(DerivedIndexMaintainer.SUFFIX),
            () -> "The failure must name the derived resource, but was: " + denied.getMessage());

        assertNull(((AbstractResourceSession<?, ?>) session).getDerivedResource(),
            "No derived resource may be installed after the denial");
        assertNotNull(projection, "The projection index the declaration builds on stays untouched");
        assertThrows(RuntimeException.class, writer::commit,
            "The half-made declaration must leave the transaction uncommittable, not committable");
        writer.rollback();
      }
    }
  }

  /**
   * The same declaration, unguarded, installs the derived resource — so the injected denial, not
   * the declaration itself, is what the test above detects.
   */
  @Test
  void theSameDeclarationSucceedsWithoutTheDenial() {
    final var path = directory.toPath().resolve("granted");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(newRecordsResource());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareProjection(session, writer);
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
            List.of("g"), List.of("sum(n)"));

        assertNotNull(((AbstractResourceSession<?, ?>) session).getDerivedResource(),
            "An accepted declaration installs the derived resource");
        assertEquals(1,
            session.getWtxIndexController(writer.getRevisionNumber())
                   .getIndexes()
                   .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE),
            "An accepted declaration leaves exactly one temporal-aggregate index");
        writer.rollback();
      }
    }
  }

  private static ResourceConfiguration newRecordsResource() {
    return ResourceConfiguration.newBuilder("records")
                                .storageType(StorageType.FILE_CHANNEL)
                                .validTimePaths("lo", "hi")
                                .storeDiffs(false)
                                .build();
  }

  private static IndexDef declareProjection(final JsonResourceSession session, final JsonNodeTrx writer) {
    writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
        [{"g":1,"n":10,"lo":"2024-01-01T00:00:00Z","hi":"2024-12-01T00:00:00Z"}]
        """), JsonNodeTrx.Commit.NO);
    final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
        List.of(field("g"), field("n"), field("lo"), field("hi")),
        List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
    session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
    return projection;
  }

  private static Path<QNm> field(final String name) {
    return Path.parse("/[]/" + name, PathParser.Type.JSON);
  }
}
