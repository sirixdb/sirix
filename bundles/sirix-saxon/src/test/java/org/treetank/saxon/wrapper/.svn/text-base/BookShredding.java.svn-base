package org.treetank.saxon.wrapper;

import java.io.File;

import javax.xml.stream.XMLEventReader;

import org.junit.Test;
import org.treetank.TestHelper;
import org.treetank.TestHelper.PATHS;
import org.treetank.access.Database;
import org.treetank.access.conf.DatabaseConfiguration;
import org.treetank.access.conf.ResourceConfiguration;
import org.treetank.access.conf.SessionConfiguration;
import org.treetank.api.IDatabase;
import org.treetank.api.ISession;
import org.treetank.api.INodeWriteTrx;
import org.treetank.service.xml.shredder.EInsert;
import org.treetank.service.xml.shredder.XMLShredder;

public final class BookShredding {

  public BookShredding() {
  }

  /** Path to books file. */
  public static final File MY_BOOKS = new File(new StringBuilder("src").append(File.separator).append("test")
    .append(File.separator).append("resources").append(File.separator).append("data").append(File.separator)
    .append("my-books.xml").toString());

  /** Books XML file. */
  public static final File BOOKS = new File("src" + File.separator + "test" + File.separator + "resources"
    + File.separator + "data" + File.separator + "books.xml");

  public static void createMyBookDB() throws Exception {
    shredder(MY_BOOKS);
  }

  public static void createBookDB() throws Exception {
    shredder(BOOKS);
  }

  private static void shredder(final File pBooks) throws Exception {
    final DatabaseConfiguration config = new DatabaseConfiguration(TestHelper.PATHS.PATH1.getFile());
    Database.truncateDatabase(config);
    Database.createDatabase(config);
    final IDatabase database = Database.openDatabase(config.getFile());
    database.createResource(new ResourceConfiguration.Builder(TestHelper.RESOURCE, PATHS.PATH1.getConfig())
      .build());
    final ISession session =
      database.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build());
    final INodeWriteTrx wtx = session.beginNodeWriteTrx();
    final XMLEventReader reader = XMLShredder.createFileReader(pBooks);
    final XMLShredder shredder = new XMLShredder(wtx, reader, EInsert.ASFIRSTCHILD);
    shredder.call();
    wtx.close();
    session.close();
  }

  @Test
  public void fakeTest() {
  }

}
