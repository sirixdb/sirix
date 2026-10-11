package io.sirix.query;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.query.bench.bitemporal.BitemporalProjections;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Explicit string-bound fixture for tests of dictionary kernels, independent of the SH1 kit's
 * types.
 */
public final class StringBoundProjection {
  private StringBoundProjection() {}

  public static void declare(final JsonResourceSession session, final JsonNodeTrx wtx, final String resource) {
    final var controller = session.getWtxIndexController(wtx.getRevisionNumber());
    if (controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PROJECTION) > 0) {
      return;
    }
    final List<String> fields = BitemporalProjections.fields(resource);
    final List<Path<QNm>> paths = new ArrayList<>(fields.size());
    final List<Type> types = new ArrayList<>(fields.size());
    for (final String field : fields) {
      paths.add(Path.parse(BitemporalProjections.ROOT_PATH + "/" + field, PathParser.Type.JSON));
      types.add("vf".equals(field) || "vt".equals(field)
          ? Type.STR
          : Type.LON);
    }
    final var writer = wtx.getStorageEngineWriter();
    final int index = writer.getProjectionIndexPage(writer.getActualRevisionRootPage()).nextUnallocatedIndex();
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        Path.parse(BitemporalProjections.ROOT_PATH, PathParser.Type.JSON), paths, types, index, IndexDef.DbType.JSON);
    controller.createIndexes(Set.of(definition), wtx);
  }
}
