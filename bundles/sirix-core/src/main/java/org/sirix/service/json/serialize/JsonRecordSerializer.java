package org.sirix.service.json.serialize;

import com.google.common.base.Preconditions;
import org.sirix.api.json.JsonNodeReadOnlyTrx;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;

public final class JsonRecordSerializer {

  /**
   * Private constructor to prevent from instantiation.
   */
  private JsonRecordSerializer() {
  }

  /**
   * Serialize the first {@code numberOfRecords}, that is the first n-nodes of the 1st level.
   */
  public static void serialize(final JsonNodeReadOnlyTrx rtx, final int numberOfRecords, final Writer stream,
      final int... revisions) {
    Preconditions.checkArgument(numberOfRecords > 0, "Number must be > 0.");

    try {
      rtx.moveToDocumentRoot();

      if (rtx.hasFirstChild()) {
        rtx.moveToFirstChild();

        boolean isObject = false;

        if (rtx.isObject()) {
          isObject = true;
          stream.append("{");
        }

        if (rtx.hasFirstChild()) {
          rtx.moveToFirstChild();
          var nodeKey = rtx.getNodeKey();
          var jsonSerializer =
              new JsonSerializer.Builder(rtx.getResourceManager(), stream, revisions).startNodeKey(nodeKey)
                                                                                     .serializeStartNodeWithBrackets(
                                                                                         false)
                                                                                     .build();
          jsonSerializer.call();
          rtx.moveTo(nodeKey);

          if (rtx.hasRightSibling()) {
            for (int i = 1; i < numberOfRecords && rtx.hasRightSibling(); i++) {
              rtx.moveToRightSibling();
              nodeKey = rtx.getNodeKey();
              stream.append(",");
              jsonSerializer = new JsonSerializer.Builder(rtx.getResourceManager(), stream, revisions).startNodeKey(nodeKey)
                                                                                                      .serializeStartNodeWithBrackets(
                                                                                                          false)
                                                                                                      .build();
              jsonSerializer.call();
              rtx.moveTo(nodeKey);
            }
          }
        }

        if (isObject) {
          stream.append("}");
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
