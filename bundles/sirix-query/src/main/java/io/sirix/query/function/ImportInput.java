package io.sirix.query.function;

import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.atomic.Str;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.xml.shredder.XmlShredder;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamConstants;

/** Content validation before a load can create, replace or write its target. */
public final class ImportInput {
  private ImportInput() {
  }

  public static QueryException invalidContent(final String format) {
    return new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE,
        "Expected inline %s content; server file paths and URLs are not accepted", format);
  }

  /** Snapshot once: a lazy sequence must not supply different content after validation. */
  public static Sequence strings(final Sequence resources, final boolean xml) {
    final List<Str> contents = new ArrayList<>();
    try (final Iter inputs = resources.iterate()) {
      for (Item item; (item = inputs.next()) != null;) {
        final String content = ((Str) item).stringValue();
        if (xml) {
          validateXml(content);
        } else {
          validateJson(content);
        }
        contents.add(new Str(content));
      }
    }
    return contents.size() == 1 ? contents.getFirst() : new ItemSequence(contents.toArray(new Str[0]));
  }

  public static String json(final Sequence input) {
    final String content;
    if (input instanceof Str string) {
      content = input instanceof JsonDBItem ? new JsonPrimitive(string.stringValue()).toString() : string.stringValue();
    } else {
      final StringWriter output = new StringWriter();
      try (final StringSerializer serializer = new StringSerializer(new PrintWriter(output))) {
        serializer.serialize(input);
      }
      content = output.toString();
    }
    validateJson(content);
    return content;
  }

  public static void validateJson(final String content) {
    // skipValue validates without constructing a second object graph for a potentially large input.
    try (final JsonReader reader = new JsonReader(new StringReader(content))) {
      reader.setStrictness(Strictness.STRICT);
      if (reader.peek() == JsonToken.END_DOCUMENT) {
        throw invalidContent("JSON");
      }
      reader.skipValue();
      if (reader.peek() != JsonToken.END_DOCUMENT) {
        throw invalidContent("JSON");
      }
    } catch (final IOException | IllegalStateException failure) {
      throw invalidContent("JSON");
    }
  }

  public static void validateXml(final String content) {
    final XMLEventReader reader;
    try {
      reader = XmlShredder.createStringReader(content);
    } catch (final RuntimeException failure) {
      throw invalidContent("XML");
    }
    try {
      while (reader.hasNext()) {
        // Reject declarations rather than forwarding them to another parser. No entity resolver
        // may turn inline XML into an indirect server-file or network read.
        if (reader.nextEvent().getEventType() == XMLStreamConstants.DTD) {
          throw invalidContent("XML without a DTD");
        }
      }
    } catch (final XMLStreamException failure) {
      throw invalidContent("XML");
    } finally {
      try {
        reader.close();
      } catch (final XMLStreamException failure) {
        throw invalidContent("XML");
      }
    }
  }
}
