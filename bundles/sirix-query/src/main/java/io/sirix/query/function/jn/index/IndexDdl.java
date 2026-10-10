package io.sirix.query.function.jn.index;

import io.sirix.api.json.JsonNodeTrx;
import io.sirix.query.json.JsonDBItem;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

public final class IndexDdl {
  private IndexDdl() {}

  public static boolean usesWriter(final JsonDBItem document, final @Nullable JsonNodeTrx writer) {
    Objects.requireNonNull(document);
    final int revision = document.getTrx().getRevisionNumber();
    return writer != null && (revision >= document.getResourceSession().getMostRecentRevisionNumber()
        || writer.getStorageEngineWriter().getRevisionToRepresent() == revision);
  }
}
