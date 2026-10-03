package io.sirix.index.interval.json;

import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ValidTimeMoveTestSupport {
  public record Keys(long root, long record, long nested, long wrapper, long destination) {
  }

  private ValidTimeMoveTestSupport() {}

  public static String rows(final String duplicate) {
    final String from = "\"vf\":\"2023-01-01T00:00:00Z\"";
    final String to = "\"vt\":\"2025-01-01T00:00:00Z\"";
    final String bounds = duplicate.equals("vf")
        ? from + "," + from + "," + from + "," + to
        : from + "," + to + "," + to + "," + to;
    return "[{\"id\":1," + bounds + ",\"nested\":{" + exactRecordFields(2) + "}},{\"dest\":[]}]";
  }

  private static String exactRecordFields(final long id) {
    return "\"id\":" + id + ",\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"";
  }

  public static Keys keys(final JsonNodeReadOnlyTrx cursor) {
    cursor.moveToDocumentRoot();
    assertTrue(cursor.moveToFirstChild());
    final long root = cursor.getNodeKey();
    final long record = objectKey(cursor, 1);
    final long nested = objectKey(cursor, 2);
    assertTrue(cursor.moveTo(root));
    assertTrue(cursor.moveToLastChild());
    final long wrapper = cursor.getNodeKey();
    assertTrue(cursor.moveToFirstChild());
    assertTrue(cursor.isArray());
    return new Keys(root, record, nested, wrapper, cursor.getNodeKey());
  }

  public static long objectKey(final JsonNodeReadOnlyTrx cursor, final long id) {
    cursor.moveToDocumentRoot();
    final var descendants = new DescendantAxis(cursor);
    while (descendants.hasNext()) {
      descendants.nextLong();
      if (cursor.getKind() == NodeKind.OBJECT_NAMED_NUMBER
          && "id".equals(requireNonNull(cursor.getName()).getLocalName())
          && cursor.getNumberValue().longValue() == id) {
        assertTrue(cursor.moveToParent());
        return cursor.getNodeKey();
      }
    }
    throw new AssertionError("Missing record " + id);
  }

  public static long ownerKey(final JsonNodeReadOnlyTrx cursor, final long wrapper) {
    assertTrue(cursor.moveTo(wrapper));
    assertTrue(cursor.moveToFirstChild());
    assertTrue(cursor.getKind() == NodeKind.OBJECT_NAMED_OBJECT);
    assertTrue("owner".equals(requireNonNull(cursor.getName()).getLocalName()));
    return cursor.getNodeKey();
  }

  public static void mutate(final JsonNodeTrx writer, final Keys keys, final String mode, final int step) {
    switch (step) {
      case 1 -> moveInto(writer, keys.record(), keys.destination(), mode);
      case 2 -> moveInto(writer, keys.record(), keys.root(), mode);
      case 3 -> {
        assertTrue(writer.moveTo(keys.root()));
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{" + exactRecordFields(3) + "}"),
            JsonNodeTrx.Commit.NO);
        final long third = objectKey(writer, 3);
        assertTrue(writer.moveTo(keys.root()));
        writer.moveSubtreeToFirstChild(keys.record());
        switch (mode) {
          case "first" -> {
            assertTrue(writer.moveTo(keys.root()));
            writer.moveSubtreeToFirstChild(third);
          }
          case "right" -> {
            assertTrue(writer.moveTo(third));
            writer.moveSubtreeToRightSibling(keys.record());
          }
          case "left" -> {
            assertTrue(writer.moveTo(keys.record()));
            writer.moveSubtreeToLeftSibling(third);
          }
          default -> throw new IllegalArgumentException(mode);
        }
      }
      case 4 -> {
        assertTrue(writer.moveTo(keys.wrapper()));
        writer.insertObjectRecordAsFirstChild("owner", ObjectValue.INSTANCE);
        final long owner = writer.getNodeKey();
        writer.insertObjectRecordAsFirstChild("anchor", NullValue.INSTANCE);
        moveInto(writer, keys.nested(), owner, mode);
      }
      case 5 -> {
        assertTrue(writer.moveTo(keys.destination()));
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{" + exactRecordFields(4) + "}"),
            JsonNodeTrx.Commit.NO);
        moveInto(writer, keys.destination(), keys.nested(), mode);
      }
      default -> throw new IllegalArgumentException("Unknown move step " + step);
    }
  }

  private static void moveInto(final JsonNodeTrx writer, final long node, final long container, final String mode) {
    assertTrue(writer.moveTo(container));
    switch (mode) {
      case "first" -> writer.moveSubtreeToFirstChild(node);
      case "right", "left" -> {
        if (!writer.moveToFirstChild()) {
          writer.insertNullValueAsFirstChild();
        }
        if (mode.equals("right")) {
          writer.moveSubtreeToRightSibling(node);
        } else {
          writer.moveSubtreeToLeftSibling(node);
        }
      }
      default -> throw new IllegalArgumentException(mode);
    }
  }
}
