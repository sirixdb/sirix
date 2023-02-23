package org.sirix.xquery.function.xml.diff;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.jdm.Iter;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sirix.XmlTestHelper;
import org.sirix.XmlTestHelper.PATHS;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.axis.DescendantAxis;
import org.sirix.diff.service.FMSEImport;
import org.sirix.exception.SirixException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.service.InsertPosition;
import org.sirix.service.xml.serialize.XmlSerializer;
import org.sirix.service.xml.serialize.XmlSerializer.XmlSerializerBuilder;
import org.sirix.service.xml.shredder.XmlShredder;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.node.BasicXmlDBStore;

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
    try (final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      XmlResourceSession resource = database.beginResourceSession(XmlTestHelper.RESOURCE);
      Predicate<Path> fileNameFilter = path -> path.getFileName().toString().endsWith(".xml");
      final List<Path> list = Files.list(folder).filter(fileNameFilter).sorted((first, second) -> {
        final String firstName =
            first.getFileName().toString().substring(0, first.getFileName().toString().indexOf('.'));
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
            try (final XmlNodeTrx wtx = resource.beginNodeTrx();
                final FileInputStream fis = new FileInputStream(file.toFile())) {
              final XmlShredder shredder = new XmlShredder.Builder(wtx, XmlShredder.createFileReader(fis),
                  InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
              shredder.call();
            }
          } else {
            FMSEImport.main(
                new String[] {PATHS.PATH1.getFile().toAbsolutePath().toString(), file.toAbsolutePath().toString()});
          }

          resource.close();
          resource = database.beginResourceSession(XmlTestHelper.RESOURCE);

          System.out.println();
          System.out.println();
          System.out.println();
          System.out.println();

          final PathSummaryReader pathSummary = resource.openPathSummary();
          final var pathSummaryAxis = new DescendantAxis(pathSummary);

          while (pathSummaryAxis.hasNext()) {
            pathSummaryAxis.nextLong();

            System.out.println("nodeKey: " + pathSummary.getNodeKey());
            System.out.println("path: " + pathSummary.getPath());
            System.out.println("references: " + pathSummary.getReferences());
            System.out.println("level: " + pathSummary.getLevel());
          }

          final OutputStream out = new ByteArrayOutputStream();
          final XmlSerializer serializer = new XmlSerializerBuilder(resource, out).build();
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
        final XmlSerializer serializer =
            new XmlSerializerBuilder(resource, writer, -1).prettyPrint().serializeTimestamp(true).emitIDs().build();
        serializer.call();

        final var content = baos.toString(StandardCharsets.UTF_8);

        System.out.println(content);
      }
    }


    // Initialize query context and store.
    final var database = PATHS.PATH1.getFile();

    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      final String dbName = database.getFileName().toString();
      final String resName = XmlTestHelper.RESOURCE;

      final String xq = "xml:diff('" + dbName + "','" + resName + "',1,2)";

      final XQuery query = new XQuery(SirixCompileChain.createWithNodeStore(store), xq);

      try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        query.serialize(ctx, new PrintStream(out));
        final String content = out.toString(StandardCharsets.UTF_8);
        out.reset();

        final var contentToApply =
            "declare namespace x14ac = \"http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac\";" + "\n"
                + content;

        System.out.println(contentToApply);

        new XQuery(SirixCompileChain.createWithNodeStore(store), contentToApply).execute(ctx);

        final String xq2 = "xml:doc('" + dbName + "','" + resName + "',2)";
        new XQuery(SirixCompileChain.createWithNodeStore(store), xq2).serialize(ctx, new PrintStream(out));
        final String contentNewRev = out.toString(StandardCharsets.UTF_8);
        out.reset();

        final String xq3 = "xml:doc('" + dbName + "','" + resName + "',3)";
        new XQuery(SirixCompileChain.createWithNodeStore(store), xq3).serialize(ctx, new PrintStream(out));
        final String contentOldRev = out.toString(StandardCharsets.UTF_8);

        assertEquals(contentNewRev, contentOldRev);

        out.reset();

        final String xq4 = "xml:doc('" + dbName + "','" + resName + "',3)//*[local-name()='c' and not(previous::*)]";
        final Sequence sequence = new XQuery(SirixCompileChain.createWithNodeStore(store), xq4).execute(ctx);
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
