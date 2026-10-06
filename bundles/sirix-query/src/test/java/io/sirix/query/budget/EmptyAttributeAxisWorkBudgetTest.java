package io.sirix.query.budget;

import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.xml.ForwardingXmlNodeReadOnlyTrx;
import io.sirix.access.trx.page.AbstractForwardingStorageEngineReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBNode;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class EmptyAttributeAxisWorkBudgetTest {

  private static final int DISTINCT_NAMES = 512;

  @TempDir
  private Path directory;

  @BeforeEach
  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @ValueSource(strings = {"@missing", "attribute::attribute(missing)"})
  void emptyNamedAttributeAxisDoesNotLoadDescendantNames(final String step) throws Exception {
    final StringBuilder xml = new StringBuilder(DISTINCT_NAMES * 24).append("<root>");
    for (int i = 0; i < DISTINCT_NAMES; i++) {
      xml.append("<leaf a").append(i).append("='value'/>");
    }
    xml.append("</root>");
    try (final BasicXmlDBStore store = newStore()) {
      store.create("input", "resource1", new DocumentParser(xml.toString()));
    }

    assertColdQuery("@a0", true);
    assertColdQuery(step, false);
  }

  private void assertColdQuery(final String step, final boolean descendant) throws Exception {
    clearCaches();
    try (final BasicXmlDBStore store = newStore();
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final var session = store.lookup("input").getDatabase().beginResourceSession("resource1");
        final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToFirstChild());
      if (descendant) {
        assertTrue(rtx.moveToFirstChild());
      }
      assertEquals(descendant
          ? 1
          : 0, rtx.getAttributeCount());
      final CountingReader reader = new CountingReader(rtx.getStorageEngineReader());
      final XmlNodeReadOnlyTrx measuredTrx = new ForwardingXmlNodeReadOnlyTrx() {
        @Override
        public XmlNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
          return rtx;
        }

        @Override
        public StorageEngineReader getStorageEngineReader() {
          return reader;
        }
      };
      context.setContextItem(new XmlDBNode(measuredTrx, store.lookup("input")));
      final WorkCapture.Captured<String> query = WorkCapture.of(reader.nameRecords).call(() -> {
        final StringWriter output = new StringWriter();
        try (final PrintWriter writer = new PrintWriter(output)) {
          new Query(chain, "xquery version \"1.0\"; count(" + step + ")").serialize(context, writer);
        }
        return output.toString();
      });
      assertEquals(descendant
          ? "1"
          : "0", query.result());
      if (descendant) {
        query.work()
             .assertAtLeast(reader.nameRecords, 1,
                 "the cold nonempty attribute query must exercise the name-record counting seam");
      } else {
        query.work()
             .assertZero(reader.nameRecords,
                 "an empty attribute axis must not reconstruct dictionaries for descendant attributes: " + step);
      }
    }
  }

  private BasicXmlDBStore newStore() {
    return BasicXmlDBStore.newBuilder()
                          .location(directory)
                          .storageType(StorageType.FILE_CHANNEL)
                          .hashType(HashType.NONE)
                          .buildPathSummary(false)
                          .versioningType(VersioningType.FULL)
                          .build();
  }

  private static final class CountingReader extends AbstractForwardingStorageEngineReader {
    private final StorageEngineReader reader;
    private long records;
    private final WorkCounter nameRecords = WorkCounter.alwaysOn("nameRecords",
        "name/count records requested while reconstructing dictionaries", () -> records);

    private CountingReader(final StorageEngineReader reader) {
      this.reader = reader;
    }

    @Override
    protected StorageEngineReader delegate() {
      return reader;
    }

    // The inherited StorageEngineReader contract requires this generic return type.
    @Override
    @SuppressWarnings("TypeParameterUnusedInFormals")
    public <V extends DataRecord> V getRecord(final long key, final IndexType indexType, final int index) {
      if (indexType == IndexType.NAME) {
        records++;
      }
      return reader.getRecord(key, indexType, index);
    }
  }
}
