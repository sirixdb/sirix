package io.sirix.service.xml.serialize;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.nio.file.Path;

import static io.sirix.service.xml.serialize.XmlSerializationAssertions.assertElement;
import static io.sirix.service.xml.serialize.XmlSerializationAssertions.parse;

final class SAXSerializerEscapingTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void contentHandlerEscapesDecodedValuesExactlyOnce(final VersioningType versioning) throws Exception {
    final Path databasePath = directory.resolve("sax");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      for (int i = 0; i < XmlSerializationAssertions.ENCODED_VALUES.size(); i++) {
        database.createResource(ResourceConfiguration.newBuilder("data" + i)
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .versioningApproach(versioning)
                                                     .build());
        final String input = XmlSerializationAssertions.document(XmlSerializationAssertions.ENCODED_VALUES.get(i));
        try (final var session = database.beginResourceSession("data" + i)) {
          try (final var writer = session.beginNodeTrx()) {
            writer.insertSubtreeAsFirstChild(XmlShredder.createStringReader(input));
            writer.commit();
          }
          final var factory = (SAXTransformerFactory) TransformerFactory.newInstance();
          final var handler = factory.newTransformerHandler();
          handler.getTransformer().setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
          final StringWriter output = new StringWriter();
          handler.setResult(new StreamResult(output));
          new SAXSerializer(session, handler, session.getMostRecentRevisionNumber()).call();
          assertElement(parse(input).getDocumentElement(), parse(output.toString()).getDocumentElement(), false);
        }
      }
    } finally {
      Databases.removeDatabase(databasePath);
    }
  }
}
