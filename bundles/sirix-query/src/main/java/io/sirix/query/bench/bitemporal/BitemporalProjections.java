package io.sirix.query.bench.bitemporal;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The projection (column) index each SH1 business resource declares at E0, beside its valid-time
 * index: every payload field (the integer codes {@code category} and {@code region} included) as a
 * {@code long} column and the two valid-time bounds as string columns (the kit writes fixed-width
 * UTC instants, which sort as time). The index is maintained incrementally by every later
 * publication's commit, and a reader at revision {@code r} sees it as of {@code r}, so a query at
 * any publication reads its columns.
 *
 * <p>
 * This is what turns the grouped SH1 queries (Q6-Q9, Q11, Q12) into column scans: the valid-time
 * index yields the record keys of the segments valid at the query's instant, and the projection
 * executor folds exactly those rows' {@code grade}, {@code qty}, {@code cost} and {@code sid}
 * columns — no segment object is materialised. The declaration itself is a kit configuration, not a
 * query-time choice.
 * </p>
 */
public final class BitemporalProjections {

  /** The array members of a business resource: the records a projection row stands for. */
  public static final String ROOT_PATH = "/[]";

  private BitemporalProjections() {
    throw new AssertionError("no instances");
  }

  /**
   * The projected field names of {@code resource}, payload fields first, then the valid-time bounds.
   */
  public static List<String> fields(final String resource) {
    return switch (Objects.requireNonNull(resource, "resource")) {
      case BitemporalSchema.CONTRACTS -> List.of("id", "pid", "sid", "cost", "qty", "grade", "vf", "vt");
      case BitemporalSchema.PRODUCTS -> List.of("id", "category", "retail", "vf", "vt");
      case BitemporalSchema.SUPPLIERS -> List.of("id", "region", "tier", "vf", "vt");
      default -> throw new IllegalArgumentException("not a business resource: " + resource);
    };
  }

  /**
   * The declared column type of {@code field}: the two valid-time bounds are strings (the kit writes
   * fixed-width UTC instants, which sort as time); every payload field, {@code category} and
   * {@code region} included, is an integer code in the SH1 stream and a {@code long} column.
   */
  public static Type type(final String field) {
    return switch (field) {
      case "vf", "vt" -> Type.STR;
      default -> Type.LON;
    };
  }

  /**
   * Declare the projection over {@code resource}'s array members inside {@code wtx}, which the caller
   * commits (the E0 commit): the index is built from the inserted records and catalogued with them,
   * so the first revision already carries its columns. Idempotent per resource: a second call on a
   * resource that already declares a projection does nothing.
   *
   * @param session the resource session the write transaction belongs to
   * @param wtx the open write transaction holding the shredded E0 array
   * @param resource the business resource name
   */
  public static void declare(final JsonResourceSession session, final JsonNodeTrx wtx, final String resource) {
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(wtx, "wtx");
    final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
    if (controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PROJECTION) > 0) {
      return;
    }
    final List<String> fields = fields(resource);
    final List<Path<QNm>> fieldPaths = new ArrayList<>(fields.size());
    final List<Type> fieldTypes = new ArrayList<>(fields.size());
    for (final String field : fields) {
      fieldPaths.add(Path.parse(ROOT_PATH + "/" + field, PathParser.Type.JSON));
      fieldTypes.add(type(field));
    }
    final StorageEngineWriter writer = wtx.getStorageEngineWriter();
    final int indexNumber = writer.getProjectionIndexPage(writer.getActualRevisionRootPage()).nextUnallocatedIndex();
    final IndexDef definition = IndexDefs.createProjectionIdxDef(Path.parse(ROOT_PATH, PathParser.Type.JSON),
        fieldPaths, fieldTypes, indexNumber, IndexDef.DbType.JSON);
    controller.createIndexes(Set.of(definition), wtx);
  }
}
