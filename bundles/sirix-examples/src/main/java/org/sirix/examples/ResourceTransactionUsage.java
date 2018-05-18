package org.sirix.examples;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.xml.stream.XMLStreamException;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.shredder.XMLShredder;

public final class ResourceTransactionUsage {

  /** User home directory. */
  private static final String USER_HOME = System.getProperty("user.home");

  /** Storage for databases: Sirix data in home directory. */
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  public static void main(final String[] args) {
    final Path file = LOCATION.resolve("db");
    final DatabaseConfiguration config = new DatabaseConfiguration(file);
    if (Files.exists(file)) {
      Databases.removeDatabase(file);
    }
    Databases.createDatabase(config);

    try (final Database database = Databases.openDatabase(file)) {
      database.createResource(new ResourceConfiguration.Builder("resource", config).build());

      try (final ResourceManager resource = database.getResourceManager("resource");
          final XdmNodeWriteTrx wtx = resource.beginNodeWriteTrx()) {
        wtx.insertSubtreeAsFirstChild(XMLShredder.createFileReader(LOCATION.resolve("input.xml")));
        wtx.moveTo(2);
        wtx.moveSubtreeToFirstChild(4).commit();

        final OutputStream out = new ByteArrayOutputStream();
        new XMLSerializer.XMLSerializerBuilder(resource, out).prettyPrint().build().call();

        System.out.println(out);
      }
    } catch (final SirixException | IOException | XMLStreamException e) {
      // LOG or do anything, the database is closed properly.
    }
  }
}
