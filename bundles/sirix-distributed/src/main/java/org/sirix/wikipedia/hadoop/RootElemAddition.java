package org.sirix.wikipedia.hadoop;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

public class RootElemAddition {
  public static void main(final String[] pArgs) throws XMLStreamException, FactoryConfigurationError,
    IOException {
    final FileWriter writer = new FileWriter(new File("target", "wikipedia-50-articles-sorted.xml"));
    writer.write("<mediawiki xmlns='http://www.mediawiki.org/xml/export-0.5/'>");
    final FileReader reader = new FileReader(pArgs[0]);
    for (int c, i = 0; (c = reader.read()) != -1; i++) {
      writer.write(c);
      if (i % 1_000_000 == 0) {
        writer.flush();
      }
    }
    reader.close();
    writer.write("</mediawiki>");
    writer.flush();
    writer.close();
  }
}
