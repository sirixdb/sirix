package org.sirix.examples;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
			loadDocumentAndQuery();
			System.out.println();
			loadDocumentAndUpdate();
			System.out.println();
			loadCollectionAndQuery();
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
		String xq1 = String.format("bit:load('mydoc.xml', '%s')", doc);
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
			final String xq1 = String.format("bit:load('mydoc.xml', '%s')", doc);
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
					"bit:load('mydocs.col', io:ls('%s', '\\.xml$'), fn:false())", dir);
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
			final String xq1 = String.format("bit:load('mydocs.col', '%s')", doc1);
			System.out.println(xq1);
			new XQuery(xq1).evaluate(ctx);

			// Reuse store and query loaded document.
			final QueryContext ctx2 = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq2 = "insert nodes <a><b/>test</a> into doc('mydocs.col')/log";
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
			System.out.println("Create cas index for all attributes:");
			final Sequence seq = new XQuery(
					new SirixCompileChain(store),
					"let $doc := sdb:create-cas-index('mydocs.col', 'resource1', 'xs:string', '//@*') " +
					"return sdb:create-cas-index-from-doc($doc, 'xs:string', '//*')")
					.execute(ctx3);
			final Item item = seq.evaluateToItem(ctx3, seq);
			System.out.println(item);
			store.commitAll();
			System.out.println("CAS index creation done.");
		}

		// Create and commit path index on all elements.
		try (final DBStore store = DBStore.newBuilder().isUpdatable().build()) {
			final QueryContext ctx3 = new QueryContext(store);
			System.out.println();
			System.out.println("Create path index for all elements (all paths):");
			new XQuery(new SirixCompileChain(store),
					"sdb:create-path-index('mydocs.col', 'resource1', '//*')")
					.execute(ctx3);
			store.commitAll();
			System.out.println("Path index creation done.");
		}

		// Query CAS index.
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out.println("Find CAS index for all attribute values.");
			final QueryContext ctx3 = new QueryContext(store);
			final DBNode node = (DBNode) new XQuery(new SirixCompileChain(store),
					"doc('mydocs.col')").execute(ctx3);
			final Optional<IndexDef> index = node.getTrx().getSession()
					.getIndexController().getIndexes().findCASIndex(Path.parse("//@*"));
			System.out.println(index);
			final String query = "sdb:scan-cas-index('mydocs.col', 'resource1', "
					+ index.get().getID() + ", 'bar', true(), 0, ())";
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

		// Query path index for "a-elements"
		try (final DBStore store = DBStore.newBuilder().build()) {
			System.out.println("");
			System.out
					.println("Find path index for all elements which are children of the log-element.");
			final QueryContext ctx3 = new QueryContext(store);
			final DBNode node = (DBNode) new XQuery(new SirixCompileChain(store),
					"doc('mydocs.col')").execute(ctx3);
			final Optional<IndexDef> index = node.getTrx().getSession()
					.getIndexController().getIndexes()
					.findPathIndex(Path.parse("//log/*"));
			System.out.println(index);
			final String query = "sdb:scan-path-index('mydocs.col', 'resource1', "
					+ index.get().getID() + ", ())";
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

		try (final DBStore store = DBStore.newBuilder().build()) {
			final QueryContext ctx = new QueryContext(store);
			System.out.println();
			System.out.println("Query loaded document:");
			final String xq3 = "doc('mydocs.col', 2)/log/all-time::*";
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
		out.print("</log>");
		out.close();
		return file;
	}

}
