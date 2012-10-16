package org.sirix.saxon.wrapper;

import java.io.File;

import javax.xml.stream.XMLEventReader;

import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.DatabaseImpl;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.Session;
import org.sirix.api.NodeWriteTrx;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.XMLShredder;

public final class BookShredding {

	public BookShredding() {
	}

	/** Path to books file. */
	public static final File MY_BOOKS = new File(new StringBuilder("src")
			.append(File.separator).append("test").append(File.separator)
			.append("resources").append(File.separator).append("data")
			.append(File.separator).append("my-books.xml").toString());

	/** Books XML file. */
	public static final File BOOKS = new File("src" + File.separator + "test"
			+ File.separator + "resources" + File.separator + "data" + File.separator
			+ "books.xml");

	public static void createMyBookDB() throws Exception {
		shredder(MY_BOOKS);
	}

	public static void createBookDB() throws Exception {
		shredder(BOOKS);
	}

	private static void shredder(final File pBooks) throws Exception {
		final DatabaseConfiguration config = new DatabaseConfiguration(
				TestHelper.PATHS.PATH1.getFile());
		DatabaseImpl.truncateDatabase(config);
		DatabaseImpl.createDatabase(config);
		final Database database = DatabaseImpl.openDatabase(config.getFile());
		database.createResource(new ResourceConfiguration.Builder(
				TestHelper.RESOURCE, PATHS.PATH1.getConfig()).build());
		final Session session = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final NodeWriteTrx wtx = session.beginNodeWriteTrx();
		final XMLEventReader reader = XMLShredder.createFileReader(pBooks);
		final XMLShredder shredder = new XMLShredder.Builder(wtx, reader,
				Insert.ASFIRSTCHILD).commitAfterwards().build();
		shredder.call();
		wtx.close();
		session.close();
	}

	@Test
	public void fakeTest() {
	}

}
