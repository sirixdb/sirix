package org.sirix.examples;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Comparator;
import java.util.Date;
import java.util.Random;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.XQuery;
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
import org.sirix.xquery.node.DBNode;
import org.sirix.xquery.node.DBStore;

import com.google.common.base.Optional;

/**
 * A few examples (some taken from the official brackit examples). Usually you
 * would use a logger for all output!
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
	 * @param args
	 *          not used
	 */
	public static void main(final String[] args) throws SirixException {
		try {
			// loadDocumentAndQuery();
			// System.out.println();
			// loadDocumentAndUpdate();
			// System.out.println();
			// loadCollectionAndQuery();
			// System.out.println();
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
	private static void loadDocumentAndQuery() throws QueryException,
			IOException, SirixException {
		final File doc = new File(new StringBuilder("src").append(File.separator)
				.append("main").append(File.separator).append("resources")
				.append(File.separator).append("test.xml").toString());

		// Initialize query context and store.
		final DBStore store = DBStore.newBuilder().build();
		QueryContext ctx = new QueryContext(store);

		// Use XQuery to load sample document into store.
		System.out.println("Loading document:");
		URI docUri = doc.toURI();
		String xq1 = String
				.format("bit:load('mydoc.xml', '%s')", docUri.toString());
		System.out.println(xq1);
		new XQuery(xq1).evaluate(ctx);

		try (final Database database = Databases.openDatabase(new File(
				new StringBuilder(3).append(LOCATION).append(File.separator)
						.append("mydoc.xml").toString()))) {
			// Reuse store and query loaded document.
			final QueryContext ctx2 = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq2 = "doc('mydoc.xml')/nachrichten/nachricht[betreff/text()='sommer' or betreff/text()='strand' or text/text()='sommer' or text/text()='strand']";
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
	private static void loadDocumentAndUpdate() throws QueryException,
			IOException {
		// Prepare sample document.
		final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
		final File doc = generateSampleDoc(tmpDir, "sample");
		doc.deleteOnExit();

		// Initialize query context and store.
		try (final DBStore store = DBStore.newBuilder().isUpdatable().build()) {
			final QueryContext ctx = new QueryContext(store);

			// Use XQuery to load sample document into store.
			System.out.println("Loading document:");
			URI docUri = doc.toURI();
			final String xq1 = String.format("bit:load('mydoc.xml', '%s')",
					docUri.toString());
			System.out.println(xq1);
			new XQuery(xq1).evaluate(ctx);

			// Reuse store and query loaded document.
			final QueryContext ctx2 = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq2 = "insert nodes <a><b/></a> into doc('mydoc.xml')/log";
			System.out.println(xq2);
			new XQuery(xq2).execute(ctx2);
			store.commitAll();
			System.out.println();
		}
	}

	/**
	 * Load a collection and query it.
	 */
	private static void loadCollectionAndQuery() throws QueryException,
			IOException {
		// Prepare directory with sample documents.
		final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
		final File dir = new File(tmpDir + File.separator + "docs"
				+ System.currentTimeMillis());
		if (!dir.mkdir()) {
			throw new IOException("Directory " + dir + " already exists");
		}
		dir.deleteOnExit();
		for (int i = 0; i < 10; i++) {
			generateSampleDoc(dir, "sample");
		}

		// Initialize query context and store.
		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx = new QueryContext(store);

			// Use XQuery to load all sample documents into store.
			System.out.println("Load collection from files:");
			final String xq1 = String.format(
					"bit:load('mydocs.col', io:ls('%s', '\\.xml$'))", dir);
			System.out.println(xq1);
			new XQuery(xq1).evaluate(ctx);

			// Reuse store and query loaded collection.
			final QueryContext ctx2 = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded collection:");
			final String xq2 = "for $log in collection('mydocs.col')/log\n"
					+ "where $log/@severity='critical'\n" + "return\n" + "<message>\n"
					+ "  <from>{$log/src/text()}</from>\n"
					+ "  <body>{$log/msg/text()}</body>\n" + "</message>\n";
			System.out.println(xq2);
			final XQuery q = new XQuery(xq2);
			q.prettyPrint();
			q.serialize(ctx2, System.out);
			System.out.println();

			// Use XQuery to load all sample documents once more into store.
			System.out.println("Load collection from files:");
			final String xq3 = String.format(
					"bit:load('mydocs.col', io:ls('%s', '\\.xml$'), fn:false())",
					dir.toString());
			System.out.println(xq3);
			new XQuery(xq3).evaluate(ctx);
		}
	}

	/**
	 * Load a document and query it (temporal enhancements).
	 */
	private static void loadDocumentAndQueryTemporal() throws QueryException,
			IOException {
		// Prepare sample document.
		File tmpDir = new File(System.getProperty("java.io.tmpdir"));

		// Initialize query context and store.
		try (final DBStore store = DBStore.newBuilder().isUpdatable().build()) {
			final QueryContext ctx = new QueryContext(store);

			File doc1 = generateSampleDoc(tmpDir, "sample1");
			doc1.deleteOnExit();

			// Use XQuery to load sample document into store.
			System.out.println("Loading document:");
			URI doc1Uri = doc1.toURI();
			final String xq1 = String.format("bit:load('mydocs.col', '%s')",
					doc1Uri.toString());
			System.out.println(xq1);
			new XQuery(xq1).evaluate(ctx);

			// Reuse store and query loaded document.
			final QueryContext ctx2 = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq2 = "insert nodes <a><b/>test<c/>55<d>22</d></a> into doc('mydocs.col')/log";
			System.out.println(xq2);
			final XQuery q = new XQuery(xq2);
			q.serialize(ctx2, System.out);
			store.commitAll();
			System.out.println();
		}

		// Create and commit CAS indexes on all attribute- and text-nodes.
		try (final DBStore store = DBStore.newBuilder().isUpdatable().build()) {
			final QueryContext ctx3 = new QueryContext(store);
			System.out.println();
			System.out
					.println("Create a cas index for all attributes and another one for text-nodes. A third one is created for all integers:");
			final XQuery q = new XQuery(
					new SirixCompileChain(store),
					"let $doc := sdb:doc('mydocs.col', 'resource1') "
							+ "let $casStats1 := sdb:create-cas-index($doc, 'xs:string', '//@*') "
							+ "let $casStats2 := sdb:create-cas-index($doc, 'xs:string', '//*') "
							+ "let $casStats3 := sdb:create-cas-index($doc, 'xs:integer', '//*') "
							+ "return <rev>{sdb:commit($doc)}</rev>");
			q.serialize(ctx3, System.out);
			System.out.println();
			System.out.println("CAS index creation done.");
		}

		// Create and commit path index on all elements.
		try (final DBStore store = DBStore.newBuilder().isUpdatable().build()) {
			final QueryContext ctx3 = new QueryContext(store);
			System.out.println();
			System.out.println("Create path index for all elements (all paths):");
			final XQuery q = new XQuery(new SirixCompileChain(store),
					"let $doc := sdb:doc('mydocs.col', 'resource1') "
							+ "let $stats := sdb:create-path-index($doc, '//*') "
							+ "return <rev>{sdb:commit($doc)}</rev>");
			q.serialize(ctx3, System.out);
			System.out.println();
			System.out.println("Path index creation done.");
		}

		// Create and commit name index on all elements with QName 'src' or 'msg'.
		try (final DBStore store = DBStore.newBuilder().isUpdatable().build()) {
			final QueryContext ctx3 = new QueryContext(store);
			System.out.println();
			System.out
					.println("Create name index for all elements with name 'src' or 'msg':");
			final XQuery q = new XQuery(
					new SirixCompileChain(store),
					"let $doc := sdb:doc('mydocs.col', 'resource1') "
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
			final String query = "let $doc := sdb:doc('mydocs.col', 'resource1') return sdb:scan-cas-index($doc, sdb:find-cas-index($doc, 'xs:string', '//@*'), 'bar', true(), 0, ())";
			final Sequence seq = new XQuery(new SirixCompileChain(store), query)
					.execute(ctx3);
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
			for (Item item = sortedIter.next(); item != null; item = sortedIter
					.next()) {
				System.out.println(item);
			}
		}

		// Query CAS index.
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out
					.println("Find CAS index for all text values which are integers between 10 and 100.");
			final QueryContext ctx3 = new QueryContext(store);
			final String query = "let $doc := sdb:doc('mydocs.col', 'resource1') return sdb:scan-cas-index-range($doc, sdb:find-cas-index($doc, 'xs:integer', '//*'), 10, 100, true(), true(), ())";
			final Sequence seq = new XQuery(new SirixCompileChain(store), query)
					.execute(ctx3);
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
			for (Item item = sortedIter.next(); item != null; item = sortedIter
					.next()) {
				System.out.println(item);
			}
		}

		// Query path index which are children of the log-element (only elements).
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out
					.println("Find path index for all elements which are children of the log-element (only elements).");
			final QueryContext ctx3 = new QueryContext(store);
			final DBNode node = (DBNode) new XQuery(new SirixCompileChain(store),
					"doc('mydocs.col')").execute(ctx3);
			final Optional<IndexDef> index = node.getTrx().getSession()
					.getRtxIndexController(node.getTrx().getRevisionNumber())
					.getIndexes().findPathIndex(Path.parse("//log/*"));
			System.out.println(index);
			// last param '()' queries whole index.
			final String query = "let $doc := sdb:doc('mydocs.col', 'resource1') "
					+ "return sdb:scan-path-index($doc, " + index.get().getID()
					+ ", '//log/*')";
			final Sequence seq = new XQuery(new SirixCompileChain(store), query)
					.execute(ctx3);
			final Comparator<Tuple> comparator = new Comparator<Tuple>() {
				@Override
				public int compare(Tuple o1, Tuple o2) {
					return ((Node<?>) o1).cmp((Node<?>) o2);
				}
			};
			final Sequence sortedSeq = new SortedNodeSequence(comparator, seq, true);
			final Iter sortedIter = sortedSeq.iterate();

			System.out.println("Sorted index entries in document order: ");
			for (Item item = sortedIter.next(); item != null; item = sortedIter
					.next()) {
				System.out.println(item);
			}
		}

		// Query name index.
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out.println("Find name index.");
			final QueryContext ctx3 = new QueryContext(store);
			final String query = "let $doc := sdb:doc('mydocs.col', 'resource1') return sdb:scan-name-index($doc, sdb:find-name-index($doc, fn:QName((), 'src')), fn:QName((), 'src'))";
			final Sequence seq = new XQuery(new SirixCompileChain(store), query)
					.execute(ctx3);
			final Comparator<Tuple> comparator = new Comparator<Tuple>() {
				@Override
				public int compare(Tuple o1, Tuple o2) {
					return ((Node<?>) o1).cmp((Node<?>) o2);
				}
			};
			final Sequence sortedSeq = new SortedNodeSequence(comparator, seq, true);
			final Iter sortedIter = sortedSeq.iterate();

			System.out.println("Sorted index entries in document order: ");
			for (Item item = sortedIter.next(); item != null; item = sortedIter
					.next()) {
				System.out.println(item);
			}
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
			final String xq4 = "bit:serialize(doc('mydocs.col', 1))";
			q = new XQuery(xq4);
			try (final PrintStream out = new PrintStream(new FileOutputStream(
					new File(new StringBuilder(LOCATION.getAbsolutePath())
							.append(File.separator).append("output-revision-1.xml")
							.toString())))) {
				q.prettyPrint().serialize(ctx4, out);
			}
			System.out.println();
			// Serialize second version to XML
			// ($user.home$/sirix-data/output-revision-1.xml).
			final QueryContext ctx5 = new QueryContext(store);
			final String xq5 = "bit:serialize(doc('mydocs.col', 2))";
			q = new XQuery(xq5);
			try (final PrintStream out = new PrintStream(new FileOutputStream(
					new File(new StringBuilder(LOCATION.getAbsolutePath())
							.append(File.separator).append(File.separator)
							.append("output-revision-2.xml").toString())))) {
				q.prettyPrint().serialize(ctx5, out);
			}
			System.out.println();
		}

		try (final DBStore store = DBStore.newBuilder().isUpdatable().build()) {
			final File doc = new File(new StringBuilder("src").append(File.separator)
					.append("main").append(File.separator).append("resources")
					.append(File.separator).append("test.xml").toString());

			final QueryContext ctx = new QueryContext(store);
			System.out.println();
			URI docUri = doc.toURI();
			final String xq3 = String.format(
					"sdb:load('mycoll.col', 'mydoc.xml', '%s')", docUri.toString());
			System.out.println(xq3);
			final XQuery q = new XQuery(new SirixCompileChain(store), xq3);
			q.execute(ctx);
		}
	}

	/**
	 * Generate a small sample document.
	 * 
	 * @param dir
	 *          the directory
	 * @param prefix
	 *          prefix of name to use
	 * @return the generated file
	 * @throws IOException
	 *           if any I/O exception occured
	 */
	private static File generateSampleDoc(final File dir, final String prefix)
			throws IOException {
		final File file = File.createTempFile(prefix, ".xml", dir);
		file.deleteOnExit();
		final PrintStream out = new PrintStream(new FileOutputStream(file));
		final Random rnd = new Random();
		final long now = System.currentTimeMillis();
		final int diff = rnd.nextInt(6000 * 60 * 24 * 7);
		final Date tst = new Date(now - diff);
		final Severity sev = Severity.values()[rnd.nextInt(3)];
		final String src = "192.168." + (1 + rnd.nextInt(254)) + "."
				+ (1 + rnd.nextInt(254));
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
		final String msg = new String(bytes);
		out.print("<?xml version='1.0'?>");
		out.print(String.format("<log tstamp='%s' severity='%s' foo='bar'>", tst,
				sev));
		out.print(String.format("<src>%s</src>", src));
		out.print(String.format("<msg>%s</msg>", msg));
		out.print("oops1");
		out.print("<b/>");
		out.print("oops2");
		out.print("</log>");
		out.close();
		return file;
	}

}
