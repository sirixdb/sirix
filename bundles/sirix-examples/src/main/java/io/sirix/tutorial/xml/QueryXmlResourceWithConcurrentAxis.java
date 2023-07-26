package io.sirix.tutorial.xml;

import io.sirix.tutorial.Constants;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Axis;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.NestedAxis;
import io.sirix.axis.concurrent.ConcurrentAxis;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.axis.filter.xml.XmlNameFilter;
import io.sirix.service.xml.serialize.XmlSerializer;
import io.sirix.service.xml.shredder.XmlShredder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Note that this simple example shows, that the higher level XQuery-API is much more user-friendly, when chaining
 * axis is required.
 */
public class QueryXmlResourceWithConcurrentAxis {

  private static final Path XML = Paths.get("src", "main", "resources", "xml");

  private static final Path DATABASE_PATH = Constants.SIRIX_DATA_LOCATION.resolve("xml-xmark-database");

  public static void main(String[] args) throws IOException {
    createXmlDatabase();

    queryXmlDatabase();
  }

  static void createXmlDatabase() throws IOException {
    final var pathToXmlFile = XML.resolve("10mb.xml");

    if (Files.exists(DATABASE_PATH))
      Databases.removeDatabase(DATABASE_PATH);

    final var dbConfig = new DatabaseConfiguration(DATABASE_PATH);
    Databases.createXmlDatabase(dbConfig);
    try (final var database = Databases.openXmlDatabase(DATABASE_PATH)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .useTextCompression(false)
                                                   .useDeweyIDs(true)
                                                   .build());
      try (final var manager = database.beginResourceSession("resource");
           final var wtx = manager.beginNodeTrx();
           final var fis = new FileInputStream(pathToXmlFile.toFile())) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createFileReader(fis));
        wtx.commit();
      }
    }
  }

  static void queryXmlDatabase() {
    try (final var database = Databases.openXmlDatabase(DATABASE_PATH);
         final var manager = database.beginResourceSession("resource");
         final var firstConcurrRtx = manager.beginNodeReadOnlyTrx();
         final var secondConcurrRtx = manager.beginNodeReadOnlyTrx();
         final var thirdConcurrRtx = manager.beginNodeReadOnlyTrx();
         final var firstRtx = manager.beginNodeReadOnlyTrx();
         final var secondRtx = manager.beginNodeReadOnlyTrx();
         final var thirdRtx = manager.beginNodeReadOnlyTrx()) {

      /* query: //regions/africa//location */
      final Axis axis = new NestedAxis(new NestedAxis(new ConcurrentAxis<>(firstConcurrRtx,
                                                                           new FilterAxis<>(new DescendantAxis(firstRtx,
                                                                                                               IncludeSelf.YES),
                                                                                            new XmlNameFilter(firstRtx,
                                                                                                              "regions"))),
                                                      new ConcurrentAxis<>(secondConcurrRtx,
                                                                           new FilterAxis<>(new ChildAxis(secondRtx),
                                                                                            new XmlNameFilter(secondRtx,
                                                                                                              "africa")))),
                                       new ConcurrentAxis<>(thirdConcurrRtx,
                                                            new FilterAxis<>(new DescendantAxis(thirdRtx,
                                                                                                IncludeSelf.YES),
                                                                             new XmlNameFilter(thirdRtx, "location"))));

      while (axis.hasNext()) {
        axis.nextLong();
        final var outputStream = new ByteArrayOutputStream();
        final var serializer = XmlSerializer.newBuilder(manager, outputStream)
                                            .emitIDs()
                                            .prettyPrint()
                                            .startNodeKey(axis.getTrx().getNodeKey())
                                            .build();
        serializer.call();
        final var utf8Encoding = StandardCharsets.UTF_8.toString();
        try {
          System.out.println(outputStream.toString(utf8Encoding));
        } catch (UnsupportedEncodingException e) {
        }
      }
    }
  }
}
