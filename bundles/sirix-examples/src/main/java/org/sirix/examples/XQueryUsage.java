package org.sirix.examples;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import java.util.Random;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.compiler.CompileChain;
import org.brackit.xquery.sequence.SortedNodeSequence;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Node;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.access.Databases;
import org.sirix.api.Database;
import org.sirix.exception.SirixException;
import org.sirix.index.IndexDef;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.SirixXQuery;
import org.sirix.xquery.node.DBNode;
import org.sirix.xquery.node.DBStore;

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
	private static final File LOCATION = new File(USER_HOME, "sirix-data");

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
			loadDocumentAndQuery();
			System.out.println();
			loadDocumentAndUpdate();
			System.out.println();
			// loadCollectionAndQuery();
			System.out.println();
			loadDocumentAndQueryTemporal();
		} catch (IOException e) {
			System.err.print("I/O error: ");
			System.err.println(e.getMessage());
		} catch (QueryException e) {
			System.err.print("XQuery error ");
			System.err.println(e.getMessage());
		}
	}

	/**
	 * Load a document and query it.
	 */
	private static void loadDocumentAndQuery() throws QueryException, IOException, SirixException {
		final File doc = new File(
				new StringBuilder("src").append(File.separator).append("main").append(File.separator)
						.append("resources").append(File.separator).append("test.xml").toString());

		// Initialize query context and store.
		final DBStore store = DBStore.newBuilder().build();
		QueryContext ctx = new QueryContext(store);

		// Use XQuery to load sample document into store.
		System.out.println("Loading document:");
		URI docUri = doc.toURI();
		String xq1 = String.format("bit:load('mydoc.xml', '%s')", docUri.toString());
		System.out.println(xq1);
		new XQuery(xq1).evaluate(ctx);

		try (final Database database = Databases.openDatabase(new File(new StringBuilder(3)
				.append(LOCATION).append(File.separator).append("mydoc.xml").toString()))) {
			// Reuse store and query loaded document.
			final QueryContext ctx2 = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq2 =
					"doc('mydoc.xml')/nachrichten/nachricht[betreff/text()='sommer' or betreff/text()='strand' or text/text()='sommer' or text/text()='strand']";
			System.out.println(xq2);
			XQuery query = new XQuery(new SirixCompileChain(store), xq2);
			query.prettyPrint().serialize(ctx2, System.out);
		}

		System.out.println();
		store.close();
	}

	/**
	 * Load a document and update it.
	 */
	private static void loadDocumentAndUpdate() throws QueryException, IOException {
		// Prepare sample document.
		final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
		final File doc = generateSampleDoc(tmpDir, "sample");
		doc.deleteOnExit();

		// Initialize query context and store.
		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx = new QueryContext(store);

			// Use XQuery to load sample document into store.
			System.out.println("Loading document:");
			URI docUri = doc.toURI();
			final String xq1 =
					String.format("sdb:load('mycol.xml', 'mydoc.xml', '%s')", docUri.toString());
			System.out.println(xq1);
			new XQuery(xq1).evaluate(ctx);

			// Reuse store and query loaded document.
			final QueryContext ctx2 = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq2 = "let $doc := sdb:doc('mycol.xml', 'mydoc.xml')\n"
					+ "for $log in $doc/log return \n" + "( insert nodes <a><b/></a> into $log )\n";
			System.out.println(xq2);
			new SirixXQuery(xq2).execute(ctx2);

			final SirixXQuery query = new SirixXQuery("sdb:doc('mycol.xml', 'mydoc.xml')");
			query.prettyPrint().serialize(ctx2, System.out);
			// store.commitAll();
			System.out.println();
		}
	}

	/**
	 * Load a document and query it (temporal enhancements).
	 */
	private static void loadDocumentAndQueryTemporal()
			throws QueryException, IOException, SirixException {
		// Prepare sample document.
		final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

		// Initialize query context and store (implicit transaction commit).
		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx1 = new SirixQueryContext(store);
			final CompileChain compileChain = new SirixCompileChain(store);

			final File doc1 = generateSampleDoc(tmpDir, "sample1");
			doc1.deleteOnExit();

			final URI docUri = doc1.toURI();

			// Use XQuery to load sample document into store.
			System.out.println("Loading document:");
			final String xq1 =
					String.format("sdb:load('mydocs.col', 'resource1', '%s')", docUri.toString());
			System.out.println(xq1);
			new XQuery(compileChain, xq1).evaluate(ctx1);
		}

		// try (final DBStore store= DBStore.newBuilder().build();){
		// final CompileChain compileChain = new SirixCompileChain(store);
		// final QueryContext ctx1 = new SirixQueryContext(store);
		// final String query1
		// ="replace node doc('mydocs.col')/log/src with <node>aaa</node>";
		// new XQuery(compileChain, query1).evaluate(ctx1);
		// final QueryContext ctx2 = new SirixQueryContext(store);
		// final String query2
		// ="insert nodes <ab>abc</ab> into doc('mydocs.col')/log/content";
		// new XQuery(compileChain, query2).evaluate(ctx2);
		// System.out.println();
		// System.out.println("Query loaded document:");
		// final String xq3 = "doc('mydocs.col')/log/all-time::*";
		// System.out.println(xq3);
		// XQuery q = new XQuery(new SirixCompileChain(store), xq3);
		// q.prettyPrint();
		// q.serialize(new SirixQueryContext(store), System.out);
		// }

		// // Initialize query context and store (implicit transaction commit).
		// try (final DBStore store = DBStore.newBuilder().build()) {
		// final QueryContext ctx1 = new SirixQueryContext(store);
		// final CompileChain compileChain = new SirixCompileChain(store);
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
		// // final QueryContext ctx2 = new SirixQueryContext(store);
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
		// final QueryContext ctx3 = new SirixQueryContext(store);
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

		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq3 = "let $doc:= doc('mydocs.col')/log return sdb:select-node($doc, 7) ";
			System.out.println(xq3);
			XQuery q = new XQuery(new SirixCompileChain(store), xq3);
			q.prettyPrint();
			q.serialize(ctx, System.out);
		}

		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq3 = "doc('mydocs.col')/log/all-time::*";
			System.out.println(xq3);
			XQuery q = new XQuery(new SirixCompileChain(store), xq3);
			q.prettyPrint();
			q.serialize(ctx, System.out);
		}

		// // Initialize query context and store (explicit transaction commit).
		// try (final DBStore store = DBStore.newBuilder().build()) {
		// final QueryContext ctx = new QueryContext(store);
		// final CompileChain compileChain = new SirixCompileChain(store);
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
		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx3 = new QueryContext(store);
			System.out.println();
			System.out.println(
					"Create a cas index for all attributes and another one for text-nodes. A third one is created for all integers:");
			final XQuery q = new XQuery(new SirixCompileChain(store),
					"let $doc := sdb:doc('mydocs.col', 'resource1', (), fn:boolean(1)) "
							+ "let $casStats1 := sdb:create-cas-index($doc, 'xs:string', '//@*') "
							+ "let $casStats2 := sdb:create-cas-index($doc, 'xs:string', '//*') "
							+ "let $casStats3 := sdb:create-cas-index($doc, 'xs:integer', '//*') "
							+ "return <rev>{sdb:commit($doc)}</rev>");
			q.serialize(ctx3, System.out);
			System.out.println();
			System.out.println("CAS index creation done.");
		}

		// Create and commit path index on all elements.
		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx3 = new QueryContext(store);
			System.out.println();
			System.out.println("Create path index for all elements (all paths):");
			final XQuery q = new XQuery(new SirixCompileChain(store),
					"let $doc := sdb:doc('mydocs.col', 'resource1', (), fn:boolean(1)) "
							+ "let $stats := sdb:create-path-index($doc, '//*') "
							+ "return <rev>{sdb:commit($doc)}</rev>");
			q.serialize(ctx3, System.out);
			System.out.println();
			System.out.println("Path index creation done.");
		}

		// Create and commit name index on all elements with QName 'src' or 'msg'.
		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx3 = new QueryContext(store);
			System.out.println();
			System.out.println("Create name index for all elements with name 'src' or 'msg':");
			final XQuery q = new XQuery(new SirixCompileChain(store),
					"let $doc := sdb:doc('mydocs.col', 'resource1', (), fn:boolean(1)) "
							+ "let $stats := sdb:create-name-index($doc, fn:QName((), 'src')) "
							+ "return <rev>{sdb:commit($doc)}</rev>");
			q.serialize(ctx3, System.out);
			System.out.println();
			System.out.println("Name index creation done.");
		}

		// Query CAS index.
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out.println("Find CAS index for all attribute values.");
			final QueryContext ctx3 = new QueryContext(store);
			final String query =
					"let $doc := sdb:doc('mydocs.col', 'resource1') return sdb:scan-cas-index($doc, sdb:find-cas-index($doc, 'xs:string', '//@*'), 'bar', true(), 0, ())";
			final Sequence seq = new XQuery(new SirixCompileChain(store), query).execute(ctx3);
			// final Iter iter = seq.iterate();
			// for (Item item = iter.next(); item != null; item = iter.next()) {
			// System.out.println(item);
			// }
			final Comparator<Tuple> comparator = new Comparator<Tuple>() {
				@Override
				public int compare(Tuple o1, Tuple o2) {
					return ((Node<?>) o1).cmp((Node<?>) o2);
				}
			};
			final Sequence sortedSeq = new SortedNodeSequence(comparator, seq, true);
			final Iter sortedIter = sortedSeq.iterate();

			System.out.println("Sorted index entries in document order: ");
			for (Item item = sortedIter.next(); item != null; item = sortedIter.next()) {
				System.out.println(item);
			}
		}

		// Query CAS index.
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out
					.println("Find CAS index for all text values which are integers between 10 and 100.");
			final QueryContext ctx3 = new QueryContext(store);
			final String query =
					"let $doc := sdb:doc('mydocs.col', 'resource1') return sdb:scan-cas-index-range($doc, sdb:find-cas-index($doc, 'xs:integer', '//*'), 10, 100, true(), true(), ())";
			final Sequence seq = new XQuery(new SirixCompileChain(store), query).execute(ctx3);
			// final Iter iter = seq.iterate();
			// for (Item item = iter.next(); item != null; item = iter.next()) {
			// System.out.println(item);
			// }
			final Comparator<Tuple> comparator = new Comparator<Tuple>() {
				@Override
				public int compare(Tuple o1, Tuple o2) {
					return ((Node<?>) o1).cmp((Node<?>) o2);
				}
			};
			final Sequence sortedSeq = new SortedNodeSequence(comparator, seq, true);
			final Iter sortedIter = sortedSeq.iterate();

			System.out.println("Sorted index entries in document order: ");
			for (Item item = sortedIter.next(); item != null; item = sortedIter.next()) {
				System.out.println(item);
			}
		}

		// Query path index which are children of the log-element (only elements).
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out.println(
					"Find path index for all elements which are children of the log-element (only elements).");
			final QueryContext ctx3 = new QueryContext(store);
			final DBNode node =
					(DBNode) new XQuery(new SirixCompileChain(store), "doc('mydocs.col')").execute(ctx3);
			final Optional<IndexDef> index = node.getTrx().getResourceManager()
					.getRtxIndexController(node.getTrx().getRevisionNumber()).getIndexes()
					.findPathIndex(Path.parse("//log/*"));
			System.out.println(index);
			// last param '()' queries whole index.
			final String query = "let $doc := sdb:doc('mydocs.col', 'resource1') "
					+ "return sdb:scan-path-index($doc, " + index.get().getID() + ", '//log/*')";
			final Sequence seq = new XQuery(new SirixCompileChain(store), query).execute(ctx3);
			final Comparator<Tuple> comparator = new Comparator<Tuple>() {
				@Override
				public int compare(Tuple o1, Tuple o2) {
					return ((Node<?>) o1).cmp((Node<?>) o2);
				}
			};
			final Sequence sortedSeq = new SortedNodeSequence(comparator, seq, true);
			final Iter sortedIter = sortedSeq.iterate();

			System.out.println("Sorted index entries in document order: ");
			for (Item item = sortedIter.next(); item != null; item = sortedIter.next()) {
				System.out.println(item);
			}
		}

		// Query name index.
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out.println("Query name index (src-element).");
			final QueryContext ctx3 = new QueryContext(store);
			final String query = "let $doc := sdb:doc('mydocs.col', 'resource1')"
					+ " let $sequence := sdb:scan-name-index($doc, sdb:find-name-index($doc, fn:QName((), 'src')), fn:QName((), 'src'))"
					+ " return sdb:sort($sequence)";
			final XQuery q = new XQuery(new SirixCompileChain(store), query);
			q.prettyPrint();
			q.serialize(ctx3, System.out);
		}

		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq3 = "doc('mydocs.col')/log/all-time::*";
			System.out.println(xq3);
			XQuery q = new XQuery(new SirixCompileChain(store), xq3);
			q.prettyPrint();
			q.serialize(ctx, System.out);

			// Serialize first version to XML
			// ($user.home$/sirix-data/output-revision-1.xml).
			final QueryContext ctx4 = new QueryContext(store);
			final String xq4 = "doc('mydocs.col', 1)";
			q = new XQuery(xq4);
			try (final PrintStream out = new PrintStream(
					new FileOutputStream(new File(new StringBuilder(LOCATION.getAbsolutePath())
							.append(File.separator).append("output-revision-1.xml").toString())))) {
				q.prettyPrint().serialize(ctx4, out);
			}
			System.out.println();
			// Serialize second version to XML
			// ($user.home$/sirix-data/output-revision-1.xml).
			final QueryContext ctx5 = new QueryContext(store);
			final String xq5 =
					"sdb:serialize(doc('mydocs.col', 2), fn:boolean(1), 'output-revision-2.xml')";
			q = new XQuery(xq5);
			q.execute(ctx5);
			System.out.println();
			// Serialize first, second and third version to XML
			// ($user.home$/sirix-data/output-revisions.xml).
			final QueryContext ctx6 = new QueryContext(store);
			final String xq6 =
					"for $i in ((doc('mydocs.col', 1), doc('mydocs.col', 2), doc('mydocs.col', 3))) return $i";
			q = new XQuery(xq6);
			try (final PrintStream out = new PrintStream(
					new FileOutputStream(new File(new StringBuilder(LOCATION.getAbsolutePath())
							.append(File.separator).append("output-revisions.xml").toString())))) {
				q.prettyPrint().serialize(ctx6, out);
			}
			System.out.println();
		}

		try (final DBStore store = DBStore.newBuilder().build()) {
			final File doc = new File(
					new StringBuilder("src").append(File.separator).append("main").append(File.separator)
							.append("resources").append(File.separator).append("test.xml").toString());

			final QueryContext ctx = new QueryContext(store);
			System.out.println();
			URI docUri = doc.toURI();
			final String xq3 =
					String.format("sdb:load('mycoll.col', 'mydoc.xml', '%s')", docUri.toString());
			System.out.println(xq3);
			final XQuery q = new XQuery(new SirixCompileChain(store), xq3);
			q.execute(ctx);
		}
	}

	/**
	 * Generate a small sample document.
	 *
	 * @param dir the directory
	 * @param prefix prefix of name to use
	 * @return the generated file
	 * @throws IOException if any I/O exception occured
	 */
	private static File generateSampleDoc(final File dir, final String prefix) throws IOException {
		final File file = File.createTempFile(prefix, ".xml", dir);
		file.deleteOnExit();
		final PrintStream out = new PrintStream(new FileOutputStream(file));
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
			int wlen = 1 + rnd.nextInt(8);
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
		// out.print(String.format("<log tstamp='%s' severity='%s' foo='bar'>", tst,
		// sev));
		// out.print(String.format("<src>%s</src>", src));
		// out.print(String.format("<msg>%s</msg>", msg));
		// out.print("oops1");
		// out.print("<b/>");
		// out.print("oops2");
		// out.print("</log>");
		out.print(
				"<log tstamp=\"Sun May 04 23:29:47 CEST 2014\" severity=\"high\" foo=\"bar\">10 <src>192.168.0.1</src> 111<content> <a.txt/> </content> 5</log>");
		// out.close();
		return file;
	}

}
