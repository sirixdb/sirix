package io.sirix.query.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * Declaration of a projection index to build WHILE data is shredded, rather than by walking the
 * finished resource afterwards — see
 * {@link BasicJsonDBStore#create(String, String, com.google.gson.stream.JsonReader, ProjectionSpec)}.
 *
 * <p>
 * Spelled in exactly the vocabulary {@code jn:create-projection-index} uses, so a spec and the
 * query form of the same declaration cannot drift apart: the same root path, the same field paths,
 * and the same type names.
 *
 * @param rootPath the record set's root path, e.g. {@code /[]}
 * @param fieldPaths the projected field paths, each written from the document root
 * @param fieldTypes one type name per field — {@code long}, {@code double}, {@code decimal},
 *        {@code boolean}, {@code string}, {@code timestamp} ({@code datetime}) or {@code date}. The
 *        two temporal names declare that every value is exactly {@code YYYY-MM-DDTHH:MM:SS} or
 *        {@code YYYY-MM-DD}: the column then stores an epoch instead of the text, and a value of
 *        any other shape fails the build
 * @param expectedRows how many records the source will deliver, or {@code -1} when unknown. Only
 *        the resource-wide value dictionary's election reads it, and only to decline a column whose
 *        dictionary would not fit in its byte budget. A streaming build cannot derive this — it
 *        learns the row count when the stream ends, thousands of leaves after the election — so a
 *        caller who knows it (a generator, a documented corpus) turns a late whole-projection
 *        abandon into an early per-column decline. Wrong values are safe in both directions: too
 *        low risks the runtime cap firing later, too high only declines a column that would have
 *        fit.
 */
public record ProjectionSpec(String rootPath, List<String> fieldPaths, List<String> fieldTypes, long expectedRows) {

  /** As above, with no row-count hint. */
  public ProjectionSpec(final String rootPath, final List<String> fieldPaths, final List<String> fieldTypes) {
    this(rootPath, fieldPaths, fieldTypes, -1L);
  }

  public ProjectionSpec {
    requireNonNull(rootPath);
    fieldPaths = List.copyOf(fieldPaths);
    fieldTypes = List.copyOf(fieldTypes);
    if (fieldPaths.isEmpty()) {
      throw new IllegalArgumentException("A projection needs at least one field path");
    }
    if (fieldPaths.size() != fieldTypes.size()) {
      throw new IllegalArgumentException(
          "Field/type count mismatch: " + fieldPaths.size() + " fields vs " + fieldTypes.size() + " types");
    }
  }

  /**
   * The catalogue definition this spec declares, under projection index id 0 — a load-time build runs
   * on a fresh resource, where no other projection can already hold that id.
   */
  public IndexDef toIndexDef() {
    final List<Path<QNm>> paths = new ArrayList<>(fieldPaths.size());
    final List<Type> types = new ArrayList<>(fieldTypes.size());
    for (int i = 0; i < fieldPaths.size(); i++) {
      paths.add(Path.parse(fieldPaths.get(i), PathParser.Type.JSON));
      types.add(projectionType(fieldTypes.get(i)));
    }
    return IndexDefs.createProjectionIdxDef(Path.parse(rootPath, PathParser.Type.JSON), paths, types, 0,
        IndexDef.DbType.JSON);
  }

  private static Type projectionType(final String type) {
    return switch (type.toLowerCase(Locale.ROOT)) {
      case "long", "integer", "int" -> Type.LON;
      case "double", "float" -> Type.DBL;
      case "decimal", "dec" -> Type.DEC;
      case "boolean", "bool" -> Type.BOOL;
      case "string", "str" -> Type.STR;
      // Declared temporal columns: one canonical ISO-8601 shape per kind, stored as an epoch.
      case "timestamp", "datetime" -> Type.DATI;
      case "date" -> Type.DATE;
      default -> throw new IllegalArgumentException("Unsupported projection column type '" + type
          + "' — use long (integer/int), double (float), decimal (dec), boolean (bool), string (str), "
          + "timestamp (datetime) or date");
    };
  }
}
