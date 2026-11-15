package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Clause B3 of the acceptance contract: the query store creates hashless resources by default, and
 * an explicit {@code hashType} — on the store builder or as a {@code jn:store} option — still wins.
 *
 * <p>
 * The store builders used to hard-code {@link HashType#ROLLING}, which is how the Java API and the
 * REST server came to disagree about the default. They now resolve it from
 * {@link ResourceConfiguration#DEFAULT_HASH_TYPE}, so there is one place to change.
 *
 * <p>
 * Every assertion reads the hash kind back from the <em>persisted</em> resource settings, not from
 * the builder, so it proves what a created resource actually got.
 */
final class HashDefaultOffStoreTest {

  private static final String DATABASE = "json-path1";

  private static final String RESOURCE = "mydoc.jn";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void jsonStoreDefaultsToTheResourceConfigurationDefault() {
    assertSame(HashType.NONE, ResourceConfiguration.DEFAULT_HASH_TYPE);
    try (final var store = store(null)) {
      assertSame(HashType.NONE, store.options().hashType());
    }
    assertSame(HashType.NONE, created(null, null));
  }

  @Test
  void jsonStoreHonoursAnExplicitHashType() {
    try (final var store = store(HashType.ROLLING)) {
      assertSame(HashType.ROLLING, store.options().hashType());
    }
    assertSame(HashType.ROLLING, created(HashType.ROLLING, null));
  }

  /** The XML store resolves the same default, and still honours an explicit hash kind. */
  @Test
  void xmlStoreDefaultsToTheResourceConfigurationDefault(@TempDir final Path directory) {
    assertSame(HashType.NONE, xmlResourceHashType(directory.resolve("default"), null));
    assertSame(HashType.ROLLING, xmlResourceHashType(directory.resolve("rolling"), HashType.ROLLING));
  }

  private static HashType xmlResourceHashType(final Path location, final HashType configured) {
    final var builder = BasicXmlDBStore.newBuilder().location(location);
    if (configured != null) {
      builder.hashType(configured);
    }
    try (final BasicXmlDBStore store = builder.build()) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser("<value>one</value>"));
      try (final var session = collection.getDatabase().beginResourceSession("resource1")) {
        return session.getResourceConfig().hashType;
      }
    }
  }

  /** The {@code jn:store} option overrides the store's own default for that one resource. */
  @Test
  void storeQueryOptionOverridesTheDefault() {
    assertSame(HashType.ROLLING, created(null, HashType.ROLLING));
    JsonTestHelper.deleteEverything();
    assertSame(HashType.NONE, created(HashType.ROLLING, HashType.NONE));
  }

  /**
   * Creates a resource through {@code jn:store} and returns the hash kind it was persisted with.
   *
   * @param configured hash kind set on the store builder, or {@code null} to leave the default
   * @param option hash kind passed as a {@code jn:store} option, or {@code null} to pass none
   */
  private static HashType created(final HashType configured, final HashType option) {
    final String query = option == null
        ? "jn:store('" + DATABASE + "','" + RESOURCE + "','[1,2,3]')"
        : "jn:store('" + DATABASE + "','" + RESOURCE + "','[1,2,3]',true(),{'hashType':'" + option.name() + "'})";
    try (final var store = store(configured);
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, query).evaluate(ctx);
    }
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE)) {
      return session.getResourceConfig().hashType;
    }
  }

  private static BasicJsonDBStore store(final HashType hashType) {
    final var builder = BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent());
    if (hashType != null) {
      builder.hashType(hashType);
    }
    return builder.build();
  }
}
