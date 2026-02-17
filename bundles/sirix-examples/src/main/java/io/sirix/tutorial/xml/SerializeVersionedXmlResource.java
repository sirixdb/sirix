package io.sirix.tutorial.xml;

import java.io.ByteArrayOutputStream;

import io.sirix.access.Databases;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.service.xml.serialize.XmlSerializer;

import io.sirix.tutorial.Constants;

public class SerializeVersionedXmlResource {

  public static void main(String[] args) {
    CreateVersionedXmlResource.createXmlDatabaseWithVersionedResource();

    final var databasePath = Constants.SIRIX_DATA_LOCATION.resolve("xml-database-versioned");
    final var database = Databases.openXmlDatabase(databasePath);

    try (final var manager = database.beginResourceSession("resource")) {
      serializeRevisionOneAndTwo(manager);
      serializeMostRecentRevision(manager);
      serializeAllRevisions(manager);
    }
  }

  private static void serializeRevisionOneAndTwo(final XmlResourceSession manager) {
    final var outputStream = new ByteArrayOutputStream();
    final var serializerForRevisionOneAndTwo =
        XmlSerializer.newBuilder(manager, outputStream, 1, 2).emitIDs().prettyPrint().serializeTimestamp(true).build();
    serializerForRevisionOneAndTwo.call();
    System.out.println("Revision 1 and 2:");
    System.out.println(outputStream);
  }

  private static void serializeMostRecentRevision(final XmlResourceSession manager) {
    final var outputStream = new ByteArrayOutputStream();
    final var serializerForMostRecentRevision =
        XmlSerializer.newBuilder(manager, outputStream).emitIDs().prettyPrint().serializeTimestamp(true).build();
    serializerForMostRecentRevision.call();
    System.out.println("Most recent revision:");
    System.out.println(outputStream);
  }

  private static void serializeAllRevisions(final XmlResourceSession manager) {
    final var outputStream = new ByteArrayOutputStream();
    final var serializerForAllRevisions =
        XmlSerializer.newBuilder(manager, outputStream, -1).emitIDs().prettyPrint().serializeTimestamp(true).build();
    serializerForAllRevisions.call();
    System.out.println("All revisions:");
    System.out.println(outputStream);
  }
}
