package org.sirix.examples;

import org.brackit.xquery.*;
import org.brackit.xquery.compiler.CompileChain;
import org.brackit.xquery.jdm.Item;
import org.brackit.xquery.jdm.Iter;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.node.Node;
import org.brackit.xquery.sequence.SortedNodeSequence;
import org.sirix.exception.SirixException;
import org.sirix.index.IndexDef;
import org.sirix.query.SirixCompileChain;
import org.sirix.query.SirixQueryContext;
import org.sirix.query.SirixQueryContext.CommitStrategy;
import org.sirix.query.node.BasicXmlDBStore;
import org.sirix.query.node.XmlDBNode;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import java.util.Random;

/**
 * A few examples (some taken from the official brackit examples). Usually you would use a logger
 * for all output!
 *
 * @author Johannes Lichtenberger
 * @author Sebastian Bächle
 *
 */
public final class XQueryUsage {

  /** User home directory. */
  private static final String USER_HOME = System.getProperty("user.home");

  /** Storage for databases: Sirix data in home directory. */
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  /** Severity used to build a random sample document. */
  enum Severity {
    low, high, critical
  };

  /**
   * Main method.
   *
   * @param args not used
   */
  public static void main(final String[] args) throws SirixException {
    try {
      // loadOrgaDocumentAndQuery();
      loadDocumentAndQuery();
      System.out.println();
      loadDocumentAndUpdate();
      System.out.println();
      // loadCollectionAndQuery();
      System.out.println();
      loadDocumentAndQueryTemporal();
    } catch (final IOException e) {
      System.err.print("I/O error: ");
      System.err.println(e.getMessage());
    } catch (final QueryException e) {
      System.err.print("XQuery error ");
      System.err.println(e.getMessage());
    }
  }

  /**
   * Load a document and query it.
   */
  private static void loadDocumentAndQuery() throws QueryException, IOException, SirixException {
    // Initialize query context and store.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      final Path doc = Path.of(XQueryUsage.class.getClassLoader().getResource("test.xml").toURI());

      // Use XQuery to load sample document into store.
      System.out.println("Loading document:");
      final URI docUri = doc.toUri();
      final String xq1 = String.format("bit:load('mydoc.xml', '%s')", docUri.toString());
      System.out.println(xq1);
      new XQuery(xq1).evaluate(ctx);

      // Reuse store and query loaded document.
      final QueryContext ctx2 = SirixQueryContext.createWithNodeStore(store);
      System.out.println();
      System.out.println("Query loaded document:");
      final String xq2 = "doc('mydoc.xml')//*"; // nachrichten/nachricht[betreff/text()='sommer' or
                                                // betreff/text()='strand' or text/text()='sommer'
                                                // or text/text()='strand']";
      System.out.println(xq2);
      final XQuery query = new XQuery(SirixCompileChain.createWithNodeStore(store), xq2);
      query.prettyPrint().serialize(ctx2, System.out);

      System.out.println();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Load a document and query it.
   */
  private static void loadOrgaDocumentAndQuery() throws QueryException {
    // Initialize query context and store.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      final Path doc = Path.of(XQueryUsage.class.getClassLoader().getResource("orga.xml").toURI());

      // Use XQuery to load sample document into store.
      System.out.println("Loading document:");
      final URI docUri = doc.toUri();
      final String xq1 = String.format("xml:load('mydoc.col', 'mydoc.xml', '%s')", docUri);
      System.out.println(xq1);
      new XQuery(xq1).evaluate(ctx);

      // Reuse store and query loaded document.
      final QueryContext ctx2 = SirixQueryContext.createWithNodeStore(store);
      System.out.println();
      System.out.println("Query loaded document:");
      final String xq2 = "xml:doc('mydoc.col', 'mydoc.xml')/Organization/Project[@id='4711']/past::*"; // nachrichten/nachricht[betreff/text()='sommer'
      // or
      // betreff/text()='strand' or text/text()='sommer'
      // or text/text()='strand']";
      System.out.println(xq2);
      final XQuery query = new XQuery(SirixCompileChain.createWithNodeStore(store), xq2);
      query.prettyPrint().serialize(ctx2, System.out);

      System.out.println();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Load a document and update it.
   */
  private static void loadDocumentAndUpdate() throws QueryException, IOException {
    // Prepare sample document.
    final Path doc = generateSampleDoc("sample");

    // Initialize query context and store.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      // Use XQuery to load sample document into store.
      System.out.println("Loading document:");
      final URI docUri = doc.toUri();
      final String xq1 = String.format("sdb:load('mycol.xml', 'mydoc.xml', '%s')", docUri);
      System.out.println(xq1);
      new XQuery(xq1).evaluate(ctx);

      // Reuse store and query loaded document.
      final QueryContext ctx2 = SirixQueryContext.createWithNodeStore(store);
      System.out.println();
      System.out.println("Query loaded document:");
      final String xq2 = """
          let $doc := xml:doc('mycol.xml', 'mydoc.xml')
          for $log in $doc/log return\s
          ( insert nodes <a><b/></a> into $log )
          """;
      System.out.println(xq2);
      new XQuery(xq2).execute(ctx2);

      final XQuery query = new XQuery("xml:doc('mycol.xml', 'mydoc.xml')");
      query.prettyPrint().serialize(ctx2, System.out);
      System.out.println();
    }
  }

  /**
   * Load a document and query it (temporal enhancements).
   */
  private static void loadDocumentAndQueryTemporal() throws QueryException, IOException, SirixException {
    // Initialize query context and store (implicit transaction commit).
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx1 = SirixQueryContext.createWithNodeStore(store);
      final CompileChain compileChain = SirixCompileChain.createWithNodeStore(store);

      final Path doc1 = generateSampleDoc("sample1");

      final URI docUri = doc1.toUri();

      // Use XQuery to load sample document into store.
      System.out.println("Loading document:");
      final String xq1 = String.format("xml:load('mydocs.col', 'resource1', '%s')", docUri.toString());
      System.out.println(xq1);
      new XQuery(compileChain, xq1).evaluate(ctx1);
    }

    // try (final DBStore store= DBStore.newBuilder().build();){
    // final CompileChain compileChain = SirixCompileChain.createWithNodeStore(store);
    // final QueryContext ctx1 = SirixQueryContext.createWithNodeStore(store);
    // final String query1
    // ="replace node doc('mydocs.col')/log/src with <node>aaa</node>";
    // new XQuery(compileChain, query1).evaluate(ctx1);
    // final QueryContext ctx2 = SirixQueryContext.createWithNodeStore(store);
    // final String query2
    // ="insert nodes <ab>abc</ab> into doc('mydocs.col')/log/content";
    // new XQuery(compileChain, query2).evaluate(ctx2);
    // System.out.println();
    // System.out.println("Query loaded document:");
    // final String xq3 = "doc('mydocs.col')/log/all-times::*";
    // System.out.println(xq3);
    // XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store), xq3);
    // q.prettyPrint();
    // q.serialize(SirixQueryContext.createWithNodeStore(store), System.out);
    // }

    // // Initialize query context and store (implicit transaction commit).
    // try (final DBStore store = DBStore.newBuilder().build()) {
    // final QueryContext ctx1 = SirixQueryContext.createWithNodeStore(store);
    // final CompileChain compileChain = SirixCompileChain.createWithNodeStore(store);
    //
    // final File doc1 = generateSampleDoc(tmpDir, "sample1");
    // doc1.deleteOnExit();
    //
    // final URI docUri = doc1.toURI();
    //
    // // Use XQuery to load sample document into store.
    // System.out.println("Loading document:");
    // final String xq1 =
    // String.format("sdb:load('mydocs.col', 'resource1', '%s')",
    // docUri.toString());
    // System.out.println(xq1);
    // new XQuery(compileChain, xq1).evaluate(ctx1);
    //
    // // // Reuse store and insert into loaded document with a subsequent
    // explicit commit.
    // // final QueryContext ctx2 = SirixQueryContext.createWithNodeStore(store);
    // // System.out.println();
    // // System.out.println("Insert into loaded document:");
    // // final String xq2 =
    // "replace node doc('mydocs.col')/log/content with <a><div>hej</div></a>";
    // // System.out.println(xq2);
    // // new XQuery(compileChain, xq2).evaluate(ctx2);
    // // System.out.println();
    //
    // // Reuse store and insert into loaded document with a subsequent explicit
    // commit.
    // final QueryContext ctx3 = SirixQueryContext.createWithNodeStore(store);
    // System.out.println();
    // System.out.println("Insert into loaded document:");
    // final String xq3 =
    // "replace node doc('mydocs.col')/log/content/a with <a><div>hej</div></a>";
    // System.out.println(xq3);
    // final XQuery q = new XQuery(compileChain, xq3);
    // q.evaluate(ctx3);
    // q.serialize(ctx3, System.out);
    // System.out.println();
    // }

    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx = new BrackitQueryContext(store);
      System.out.println();
      System.out.println("Query loaded document:");
      final String xq3 = "let $doc:= doc('mydocs.col')/log return sdb:select-node($doc, 7) ";
      System.out.println(xq3);
      final XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store), xq3);
      q.prettyPrint();
      q.serialize(ctx, System.out);
    }

    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);
      System.out.println();
      System.out.println("Query loaded document:");
      final String xq3 = "doc('mydocs.col')/log/all-times::*";
      System.out.println(xq3);
      final XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store), xq3);
      q.prettyPrint();
      q.serialize(ctx, System.out);
    }

    // // Initialize query context and store (explicit transaction commit).
    // try (final DBStore store = DBStore.newBuilder().build()) {
    // final QueryContext ctx = new QueryContext(store);
    // final CompileChain compileChain = SirixCompileChain.createWithNodeStore(store);
    //
    // final File doc1 = generateSampleDoc(tmpDir, "sample1");
    // doc1.deleteOnExit();
    //
    // final URI docUri = doc1.toURI();
    //
    // // Use XQuery to load sample document into store.
    // System.out.println("Loading document:");
    // final String xq1 =
    // String.format("sdb:load('mydocs.col', 'resource1', '%s')",
    // docUri.toString());
    // System.out.println(xq1);
    // new XQuery(compileChain, xq1).evaluate(ctx);
    //
    // // Reuse store and insert into loaded document with a subsequent explicit
    // commit.
    // final QueryContext ctx2 = new QueryContext(store);
    // System.out.println();
    // System.out.println("Insert into loaded document:");
    // final String xq2 =
    // "insert nodes <a><b/>test<c/>55<d>22</d></a> into sdb:doc('mydocs.col', 'resource1', (),
    // fn:boolean(1))/log";
    // System.out.println(xq2);
    // final XQuery q1 = new XQuery(compileChain, xq2);
    // q1.execute(ctx2);
    // System.out.println("Commit changes:");
    // final String xq3 =
    // "sdb:commit(sdb:doc('mydocs.col', 'resource1', (), fn:boolean(1)))";
    // final XQuery q2 = new XQuery(compileChain, xq3);
    // q2.execute(ctx2);
    // System.out.println();
    // }

    // Create and commit CAS indexes on all attribute- and text-nodes.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx3 = new BrackitQueryContext(store);
      System.out.println();
      System.out.println(
          "Create a cas index for all attributes and another one for text-nodes. A third one is created for all integers:");
      final XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store),
          "let $doc := xml:doc('mydocs.col', 'resource1', (), fn:boolean(1)) "
              + "let $casStats1 := xml:create-cas-index($doc, 'xs:string', '//@*') "
              + "let $casStats2 := xml:create-cas-index($doc, 'xs:string', '//*') "
              + "let $casStats3 := xml:create-cas-index($doc, 'xs:integer', '//*') "
              + "return <rev>{sdb:commit($doc)}</rev>");
      q.serialize(ctx3, System.out);
      System.out.println();
      System.out.println("CAS index creation done.");
    }

    // Create and commit path index on all elements.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx3 = new BrackitQueryContext(store);
      System.out.println();
      System.out.println("Create path index for all elements (all paths):");
      final XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store),
          "let $doc := xml:doc('mydocs.col', 'resource1', (), fn:boolean(1)) "
              + "let $stats := xml:create-path-index($doc, '//*') " + "return <rev>{sdb:commit($doc)}</rev>");
      q.serialize(ctx3, System.out);
      System.out.println();
      System.out.println("Path index creation done.");
    }

    // Create and commit name index on all elements with QName 'src' or 'msg'.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx3 = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT);
      System.out.println();
      System.out.println("Create name index for all elements with name 'src' or 'msg':");
      final XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store),
          "let $doc := xml:doc('mydocs.col', 'resource1', (), fn:boolean(1)) "
              + "let $stats := xml:create-name-index($doc, fn:QName((), 'src')) "
              + "return <rev>{xml:commit($doc)}</rev>");
      q.serialize(ctx3, System.out);
      System.out.println();
      System.out.println("Name index creation done.");
    }

    // Query CAS index.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      System.out.println();
      System.out.println("Find CAS index for all attribute values.");
      final QueryContext ctx3 = SirixQueryContext.createWithNodeStore(store);
      final String query =
          "let $doc := xml:doc('mydocs.col', 'resource1') return xml:scan-cas-index($doc, sdb:find-cas-index($doc, 'xs:string', '//@*'), 'bar', true(), '==', ())";
      final Sequence seq = new XQuery(SirixCompileChain.createWithNodeStore(store), query).execute(ctx3);
      // final Iter iter = seq.iterate();
      // for (Item item = iter.next(); item != null; item = iter.next()) {
      // System.out.println(item);
      // }
      final Comparator<Tuple> comparator = (o1, o2) -> ((Node<?>) o1).cmp((Node<?>) o2);
      final Sequence sortedSeq = new SortedNodeSequence(comparator, seq, true);
      final Iter sortedIter = sortedSeq.iterate();

      System.out.println("Sorted index entries in document order: ");
      for (Item item = sortedIter.next(); item != null; item = sortedIter.next()) {
        System.out.println(item);
      }
    }

    // Query CAS index.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      System.out.println();
      System.out.println("Find CAS index for all text values which are integers between 10 and 100.");
      final QueryContext ctx3 = SirixQueryContext.createWithNodeStore(store);
      final String query =
          "let $doc := xml:doc('mydocs.col', 'resource1') return xml:scan-cas-index-range($doc, sdb:find-cas-index($doc, 'xs:integer', '//*'), 10, 100, true(), true(), ())";
      final Sequence seq = new XQuery(SirixCompileChain.createWithNodeStore(store), query).execute(ctx3);
      // final Iter iter = seq.iterate();
      // for (Item item = iter.next(); item != null; item = iter.next()) {
      // System.out.println(item);
      // }
      final Comparator<Tuple> comparator = (o1, o2) -> ((Node<?>) o1).cmp((Node<?>) o2);
      final Sequence sortedSeq = new SortedNodeSequence(comparator, seq, true);
      final Iter sortedIter = sortedSeq.iterate();

      System.out.println("Sorted index entries in document order: ");
      for (Item item = sortedIter.next(); item != null; item = sortedIter.next()) {
        System.out.println(item);
      }
    }

    // Query path index which are children of the log-element (only elements).
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      System.out.println();
      System.out.println("Find path index for all elements which are children of the log-element (only elements).");
      final QueryContext ctx3 = SirixQueryContext.createWithNodeStore(store);
      final XmlDBNode node =
          (XmlDBNode) new XQuery(SirixCompileChain.createWithNodeStore(store), "doc('mydocs.col')").execute(ctx3);
      final Optional<IndexDef> index = node.getTrx()
                                           .getResourceSession()
                                           .getRtxIndexController(node.getTrx().getRevisionNumber())
                                           .getIndexes()
                                           .findPathIndex(org.brackit.xquery.util.path.Path.parse("//log/*"));
      System.out.println(index);
      // last param '()' queries whole index.
      final String query = "let $doc := xml:doc('mydocs.col', 'resource1') " + "return xml:scan-path-index($doc, "
          + index.get().getID() + ", '//log/*')";
      final Sequence seq = new XQuery(SirixCompileChain.createWithNodeStore(store), query).execute(ctx3);
      final Comparator<Tuple> comparator = (o1, o2) -> ((Node<?>) o1).cmp((Node<?>) o2);
      final Sequence sortedSeq = new SortedNodeSequence(comparator, seq, true);
      final Iter sortedIter = sortedSeq.iterate();

      System.out.println("Sorted index entries in document order: ");
      for (Item item = sortedIter.next(); item != null; item = sortedIter.next()) {
        System.out.println(item);
      }
    }

    // Query name index.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      System.out.println();
      System.out.println("Query name index (src-element).");
      final QueryContext ctx3 = new BrackitQueryContext(store);
      final String query = "let $doc := xml:doc('mydocs.col', 'resource1')"
          + " let $sequence := xml:scan-name-index($doc, xml:find-name-index($doc, fn:QName((), 'src')), fn:QName((), 'src'))"
          + " return xml:sort($sequence)";
      final XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store), query);
      q.prettyPrint();
      q.serialize(ctx3, System.out);
    }

    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);
      System.out.println();
      System.out.println("Query loaded document:");
      final String xq3 = "doc('mydocs.col')/log/all-times::*";
      System.out.println(xq3);
      XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store), xq3);
      q.prettyPrint();
      q.serialize(ctx, System.out);

      // Serialize first version to XML
      // ($user.home$/sirix-data/output-revision-1.xml).
      final QueryContext ctx4 = new BrackitQueryContext(store);
      final String xq4 = "doc('mydocs.col', 1)";
      q = new XQuery(xq4);
      try (final PrintStream out =
          new PrintStream(new FileOutputStream(LOCATION.resolve("output-revision-1.xml").toFile()))) {
        q.prettyPrint().serialize(ctx4, out);
      }
      System.out.println();
      // Serialize second version to XML
      // ($user.home$/sirix-data/output-revision-1.xml).
      final QueryContext ctx5 = SirixQueryContext.createWithNodeStore(store);
      final String xq5 = "xml:serialize(doc('mydocs.col', 2), true(), 'output-revision-2.xml')";
      q = new XQuery(xq5);
      q.execute(ctx5);
      System.out.println();
      // Serialize first, second and third version to XML
      // ($user.home$/sirix-data/output-revisions.xml).
      final QueryContext ctx6 = SirixQueryContext.createWithNodeStore(store);
      final String xq6 = "for $i in ((doc('mydocs.col', 1), doc('mydocs.col', 2), doc('mydocs.col', 3))) return $i";
      q = new XQuery(xq6);
      try (final PrintStream out =
          new PrintStream(new FileOutputStream(LOCATION.resolve("output-revisions.xml").toFile()))) {
        q.prettyPrint().serialize(ctx6, out);
      }
      System.out.println();
    }

    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().build()) {
      final Path doc = Paths.get("src", "main", "resources", "test.xml");

      final SirixQueryContext ctx = SirixQueryContext.createWithNodeStore(store);
      System.out.println();
      final String xq3 = String.format("xml:load('mycoll.col', 'mydoc.xml', '%s')", doc.toUri().toString());
      System.out.println(xq3);
      final XQuery q = new XQuery(SirixCompileChain.createWithNodeStore(store), xq3);
      q.execute(ctx);
    }
  }

  /**
   * Generate a small sample document.
   *
   * @param prefix prefix of name to use
   * @return the generated file
   * @throws IOException if any I/O exception occured
   */
  private static Path generateSampleDoc(final String prefix) throws IOException {
    final Path file = Files.createTempFile(prefix, ".xml");
    final PrintStream out = new PrintStream(new FileOutputStream(file.toFile()));
    final Random rnd = new Random();
    final long now = System.currentTimeMillis();
    final int diff = rnd.nextInt(6000 * 60 * 24 * 7);
    new Date(now - diff);
    Severity.values();
    rnd.nextInt(3);
    rnd.nextInt(254);
    rnd.nextInt(254);
    final int mlen = 10 + rnd.nextInt(70);
    final byte[] bytes = new byte[mlen];
    int i = 0;
    while (i < mlen) {
      final int wlen = 1 + rnd.nextInt(8);
      int j = i;
      while (j < Math.min(i + wlen, mlen)) {
        bytes[j++] = (byte) ('a' + rnd.nextInt('z' - 'a' + 1));
      }
      i = j;
      if (i < mlen - 1) {
        bytes[i++] = ' ';
      }
    }
    new String(bytes);
    out.print("<?xml version='1.0'?>");
    // out.print(String.format("<log tstamp='%s' severity='%s' foo='bar'>", tst, sev));
    // out.print(String.format("<src>%s</src>", src));
    // out.print(String.format("<msg>%s</msg>", msg));
    // out.print("oops1");
    // out.print("<b/>");
    // out.print("oops2");
    // out.print("</log>");
    out.print(
        "<log tstamp=\"Sun May 04 23:29:47 CEST 2014\" severity=\"high\" foo=\"bar\">10 <src>192.168.0.1</src> 111<content> <a.txt/> </content> 5</log>");
    out.close();
    return file;
  }

}
