package org.sirix.examples;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.shredder.XMLShredder;

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
    Databases.createDatabase(config);

    try (var database = Databases.openDatabase(file)) {
      database.createResource(new ResourceConfiguration.Builder("resource", config).build());

      try (var resourceMgr = database.getResourceManager("resource");
          var wtx = resourceMgr.beginNodeWriteTrx();
          var fis = new FileInputStream(LOCATION.resolve("input.xml").toFile())) {
        wtx.insertSubtreeAsFirstChild(XMLShredder.createFileReader(fis));
        wtx.moveTo(2);
        wtx.moveSubtreeToFirstChild(4).commit();

        var out = new ByteArrayOutputStream();
        new XMLSerializer.XMLSerializerBuilder(resourceMgr, out).prettyPrint().build().call();

        System.out.println(out);
      }
    } catch (final SirixException | IOException e) {
      // LOG or do anything, the database is closed properly.
    }
  }
}
