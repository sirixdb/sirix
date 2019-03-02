package org.sirix.examples;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XmlSerializer;
import org.sirix.service.xml.shredder.XmlShredder;

public final class ResourceTransactionUsage {

  /** User home directory. */
  private static final String USER_HOME = System.getProperty("user.home");

  /** Storage for databases: Sirix data in home directory. */
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  // Under user.home a file with the name input.xml must exist for this simple example.
  public static void main(final String[] args) {
    var file = LOCATION.resolve("db");
    var config = new DatabaseConfiguration(file);
    if (Files.exists(file)) {
      Databases.removeDatabase(file);
    }
    Databases.createXmlDatabase(config);

    try (var database = Databases.openXmlDatabase(file)) {
      database.createResource(new ResourceConfiguration.Builder("resource").build());

      try (var resourceMgr = database.openResourceManager("resource");
          var wtx = resourceMgr.beginNodeTrx();
          var fis = new FileInputStream(LOCATION.resolve("input.xml").toFile())) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createFileReader(fis));
        wtx.moveTo(2);
        wtx.moveSubtreeToFirstChild(4).commit();

        var out = new ByteArrayOutputStream();
        new XmlSerializer.XmlSerializerBuilder(resourceMgr, out).prettyPrint().build().call();

        System.out.println(out);
      }
    } catch (final SirixException | IOException e) {
      // LOG or do anything, the database is closed properly.
    }
  }
}
