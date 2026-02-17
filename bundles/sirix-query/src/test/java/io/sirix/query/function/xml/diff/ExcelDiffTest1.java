package io.sirix.query.function.xml.diff;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.brackit.query.QueryContext;
import io.brackit.query.Query;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.diff.service.FMSEImport;
import io.sirix.exception.SirixException;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.service.InsertPosition;
import io.sirix.service.xml.serialize.XmlSerializer;
import io.sirix.service.xml.serialize.XmlSerializer.XmlSerializerBuilder;
import io.sirix.service.xml.shredder.XmlShredder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ExcelDiffTest1 {

  private static final Path RESOURCES = Paths.get("src", "test", "resources");

  private static final Path XML_SHEETS = RESOURCES.resolve("sheets4");

  static {
    XMLUnit.setIgnoreComments(true);
    XMLUnit.setIgnoreWhitespace(true);
  }

  @BeforeEach
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() throws SirixException, IOException {
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testSheetsSecond() throws Exception {
    test(XML_SHEETS);
  }

  /**
   * Test a folder of XML files.
   *
   * @param folder path to the files
   * @throws Exception if any exception occurs
   */
  private void test(final Path folder) throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    XmlResourceSession resourceSession = database.beginResourceSession(XmlTestHelper.RESOURCE);
    Predicate<Path> fileNameFilter = path -> path.getFileName().toString().endsWith(".xml");
    final List<Path> list = Files.list(folder).filter(fileNameFilter).sorted((first, second) -> {
      final String firstName = first.getFileName().toString().substring(0, first.getFileName().toString().indexOf('.'));
      final String secondName =
          second.getFileName().toString().substring(0, second.getFileName().toString().indexOf('.'));

      return Integer.compare(Integer.parseInt(firstName), Integer.parseInt(secondName));
    }).toList();

    // Sort files list according to file names.

    boolean first = true;

    // Shredder files.
    for (final Path file : list) {
      if (file.getFileName().toString().endsWith(".xml")) {
        if (first) {
          first = false;
          try (final XmlNodeTrx wtx = resourceSession.beginNodeTrx();
              final FileInputStream fis = new FileInputStream(file.toFile())) {
            final XmlShredder shredder = new XmlShredder.Builder(wtx, XmlShredder.createFileReader(fis),
                InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
            shredder.call();
          }
        } else {
          FMSEImport.main(
              new String[] {PATHS.PATH1.getFile().toAbsolutePath().toString(), file.toAbsolutePath().toString()});
        }

        resourceSession.close();
        resourceSession = database.beginResourceSession(XmlTestHelper.RESOURCE);

        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();

        final PathSummaryReader pathSummary = resourceSession.openPathSummary();
        final var pathSummaryAxis = new DescendantAxis(pathSummary);

        while (pathSummaryAxis.hasNext()) {
          pathSummaryAxis.nextLong();

          System.out.println("nodeKey: " + pathSummary.getNodeKey());
          System.out.println("path: " + pathSummary.getPath());
          System.out.println("references: " + pathSummary.getReferences());
          System.out.println("level: " + pathSummary.getLevel());
        }

        final OutputStream out = new ByteArrayOutputStream();
        final XmlSerializer serializer = new XmlSerializerBuilder(resourceSession, out).build();
        serializer.call();
        final StringBuilder sBuilder = XmlTestHelper.readFile(file, false);

        final Diff diff = new Diff(sBuilder.toString(), out.toString());
        final DetailedDiff detDiff = new DetailedDiff(diff);
        final List<Difference> differences = detDiff.getAllDifferences();
        for (final Difference difference : differences) {
          System.err.println("***********************");
          System.err.println(difference);
          System.err.println("***********************");
        }

        assertTrue(diff.similar(), "pieces of XML are similar " + diff);
        assertTrue(diff.identical(), "but are they identical? " + diff);
      }
    }

    try (final var baos = new ByteArrayOutputStream(); final var writer = new PrintStream(baos)) {
      final XmlSerializer serializer = new XmlSerializerBuilder(resourceSession, writer, -1).prettyPrint()
                                                                                            .serializeTimestamp(true)
                                                                                            .emitIDs()
                                                                                            .build();
      serializer.call();

      final var content = baos.toString(StandardCharsets.UTF_8);

      System.out.println(content);
    }

    // Initialize query context and store.
    final var databaseLocation = PATHS.PATH1.getFile();

    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(databaseLocation.getParent()).build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      final String dbName = databaseLocation.getFileName().toString();
      final String resName = XmlTestHelper.RESOURCE;

      final String xq = "xml:diff('" + dbName + "','" + resName + "',1,2)";

      final Query query = new Query(SirixCompileChain.createWithNodeStore(store), xq);

      try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        query.serialize(ctx, new PrintStream(out));
        String content = out.toString(StandardCharsets.UTF_8);
        out.reset();

        final var contentToApply = "xquery version \"1.0\";" + "\n"
            + "declare namespace x14ac = \"http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac\";" + "\n"
            + content;

        System.out.println(contentToApply);

        new Query(SirixCompileChain.createWithNodeStore(store), contentToApply).execute(ctx);

        final String xq2 = "xml:doc('" + dbName + "','" + resName + "',2)";
        new Query(SirixCompileChain.createWithNodeStore(store), xq2).serialize(ctx, new PrintStream(out));
        final String contentNewRev = out.toString(StandardCharsets.UTF_8);
        out.reset();

        final String xq3 = "xml:doc('" + dbName + "','" + resName + "',3)";
        new Query(SirixCompileChain.createWithNodeStore(store), xq3).serialize(ctx, new PrintStream(out));
        final String contentOldRev = out.toString(StandardCharsets.UTF_8);

        assertEquals(contentNewRev, contentOldRev);

        out.reset();
      }

      final String xq4 = "xquery version \"1.0\";xml:doc('" + dbName + "','" + resName
          + "',3)//*[local-name()='c' and not(previous::*)]";
      final Sequence sequence = new Query(SirixCompileChain.createWithNodeStore(store), xq4).execute(ctx);
      final Iter iter = sequence.iterate();

      StringSerializer serializer = new StringSerializer(System.out);

      for (var item = iter.next(); item != null; item = iter.next()) {
        serializer.serialize(item);
        System.out.println();
      }
      // final String changes = new String(out.toByteArray(), StandardCharsets.UTF_8);
      //
      // System.out.println(changes);
    }
  }

  // private static final String getXQuery() {
  // final var buffer = new StringBuilder();
  // buffer.append("declare namespace x14ac =
  // \"http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac\";");
  // buffer.append("let $doc := sdb:doc('path1','shredded', 1)");
  // buffer.append("return (");
  // buffer.append(" insert node attribute spans { \"1:9\" } into sdb:select-node($doc,170)");
  // buffer.append(")");
  // return buffer.toString();
  // }
}
