package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Una;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.module.Namespaces;
import io.brackit.query.node.parser.FragmentHelper;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.Path.Axis;
import io.brackit.query.util.path.Path.Step;
import io.brackit.query.util.path.PathParser;
import io.brackit.query.util.serialize.SubtreePrinter;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class IndexDef implements Materializable {
  private static final QNm DB_TYPE_ATTRIBUTE = new QNm("dbType");

  private static final QNm EXCLUDING_TAG = new QNm("excluding");

  private static final QNm INCLUDING_TAG = new QNm("including");

  private static final QNm NAME_TAG = new QNm("name");

  private static final QNm NAME_URI_ATTRIBUTE = new QNm("uri");

  private static final QNm NAME_PREFIX_ATTRIBUTE = new QNm("prefix");

  private static final QNm NAME_LOCAL_ATTRIBUTE = new QNm("local");

  private static final QNm PATH_TAG = new QNm("path");

  private static final QNm PATH_STEP_TAG = new QNm("step");

  private static final QNm PATH_AXIS_ATTRIBUTE = new QNm("axis");

  private static final QNm UNIQUE_ATTRIBUTE = new QNm("unique");

  private static final QNm CONTENT_TYPE_ATTRIBUTE = new QNm("keyType");

  private static final QNm TYPE_ATTRIBUTE = new QNm("type");

  private static final QNm ID_ATTRIBUTE = new QNm("id");

  private static final QNm VALID_TIME_FORMAT_ATTRIBUTE = new QNm("validTimeFormat");

  private static final String VALID_TIME_FORMAT = "5";

  private String validTimeFormat = VALID_TIME_FORMAT;

  private static final QNm DIMENSION_ATTRIBUTE = new QNm("dimension");

  private static final QNm DISTANCE_TYPE_ATTRIBUTE = new QNm("distanceType");

  private static final QNm HNSW_M_ATTRIBUTE = new QNm("hnswM");

  private static final QNm HNSW_EF_CONSTRUCTION_ATTRIBUTE = new QNm("hnswEfConstruction");

  private static final QNm HNSW_EF_SEARCH_ATTRIBUTE = new QNm("hnswEfSearch");

  // Projection-index tags
  private static final QNm PROJECTION_FIELDS_TAG = new QNm("projectionFields");
  private static final QNm PROJECTION_FIELD_TAG = new QNm("projectionField");
  private static final QNm PROJECTION_FIELD_TYPE_ATTRIBUTE = new QNm("contentType");
  private static final QNm PROJECTION_SORT_TAG = new QNm("projectionSort");
  private static final QNm PROJECTION_SORT_KEY_TAG = new QNm("keyColumn");
  private static final QNm PROJECTION_SORT_COLUMN_ATTRIBUTE = new QNm("column");

  public static final QNm INDEX_TAG = new QNm("index");

  private DbType dbType;

  private IndexType type;

  // unique flag (for CAS indexes)
  private boolean unique = false;

  // for CAS indexes
  private Type contentType;

  // populated when index is built
  private int id;

  // vector index fields
  private int dimension;

  // Store as string to avoid circular dep — "L2", "COSINE", "INNER_PRODUCT"
  private String distanceType;

  private int hnswM = 16;

  private int hnswEfConstruction = 200;

  private int hnswEfSearch = 50;

  /**
   * Projection-index state. {@link #paths} holds exactly one entry — the projection's root path (e.g.
   * {@code $doc[]}) — while {@link #projectionFields} is an ordered list of the declared sub-field
   * paths, each paired with its declared value type. The ordering matters: column layout in every
   * leaf page mirrors this list, so look-ups at scan time are
   * {@code fields.get(i) → leafPage.longCol[i]} and friends without a hash probe.
   */
  private final ArrayList<Path<QNm>> projectionFields = new ArrayList<>();

  /**
   * Per-field declared value type, index-aligned with {@link #projectionFields}. Determines the
   * leaf-page column kind: numeric → packed {@code long[]}, boolean → bit array, string → dict-id
   * {@code int[]} with a per-leaf local dictionary.
   */
  private final ArrayList<Type> projectionFieldTypes = new ArrayList<>();

  /** Optional sorted view, ordered by some of this projection's declared fields. */
  private @Nullable ProjectionSortedSpec projectionSortedSpec;

  public enum DbType {
    XML,

    JSON;

    public static Optional<DbType> ofString(String type) {
      return Arrays.stream(DbType.values()).filter(env -> env.name().equalsIgnoreCase(type)).findFirst();
    }
  }

  private final Set<Path<QNm>> paths = new HashSet<>();

  private final Set<QNm> excluded = new HashSet<>();

  private final Set<QNm> included = new HashSet<>();

  public IndexDef(DbType dbType) {
    this.dbType = dbType;
  }

  /**
   * Name index.
   */
  IndexDef(final Set<QNm> included, final Set<QNm> excluded, final int indexDefNo, final DbType dbType) {
    type = IndexType.NAME;
    this.included.addAll(included);
    this.excluded.addAll(excluded);
    id = indexDefNo;
    this.dbType = dbType;
  }

  /**
   * Path index.
   */
  IndexDef(final Set<Path<QNm>> paths, final int indexDefNo, final DbType dbType) {
    type = IndexType.PATH;
    this.paths.addAll(paths);
    id = indexDefNo;
    this.dbType = dbType;
  }

  /**
   * CAS index.
   */
  IndexDef(final Type contentType, final Set<Path<QNm>> paths, final boolean unique, final int indexDefNo,
      final DbType dbType) {
    type = IndexType.CAS;
    this.contentType = requireNonNull(contentType);
    this.paths.addAll(paths);
    this.unique = unique;
    id = indexDefNo;
    this.dbType = dbType;
  }

  /**
   * Valid-time (bitemporal) interval index. Indexes the two valid-time paths ({@code validFrom} /
   * {@code validTo}) as a persistent Relational-Interval-Tree for stabbing queries. The valid-time
   * field local names are taken from the resource's {@link io.sirix.access.ValidTimeConfig} at
   * build/maintain time; {@code paths} carries the two indexed paths (for parity with the CAS index
   * and for index identification).
   */
  IndexDef(final Set<Path<QNm>> paths, final int indexDefNo, final DbType dbType, final boolean validTimeMarker) {
    type = IndexType.VALIDTIME;
    this.contentType = Type.STR;
    this.paths.addAll(paths);
    id = indexDefNo;
    this.dbType = dbType;
  }

  /**
   * Vector index.
   */
  IndexDef(final int dimension, final String distanceType, final Set<Path<QNm>> paths, final int hnswM,
      final int hnswEfConstruction, final int indexDefNo, final DbType dbType) {
    type = IndexType.VECTOR;
    this.dimension = dimension;
    this.distanceType = requireNonNull(distanceType);
    this.paths.addAll(paths);
    this.hnswM = hnswM;
    this.hnswEfConstruction = hnswEfConstruction;
    this.hnswEfSearch = 50;
    id = indexDefNo;
    this.dbType = dbType;
  }

  /**
   * Vector index with custom efSearch.
   */
  IndexDef(final int dimension, final String distanceType, final Set<Path<QNm>> paths, final int hnswM,
      final int hnswEfConstruction, final int hnswEfSearch, final int indexDefNo, final DbType dbType) {
    type = IndexType.VECTOR;
    this.dimension = dimension;
    this.distanceType = requireNonNull(distanceType);
    this.paths.addAll(paths);
    this.hnswM = hnswM;
    this.hnswEfConstruction = hnswEfConstruction;
    this.hnswEfSearch = hnswEfSearch;
    id = indexDefNo;
    this.dbType = dbType;
  }

  /**
   * Projection index. Stores a covering index over {@code rootPath} with one column per declared
   * field in {@code fieldPaths}, in order. {@code fieldTypes} is index-aligned to {@code fieldPaths}
   * and declares each column's value type (numeric/boolean/string) so the HOT leaf layout can pick
   * the right primitive column shape.
   *
   * @param rootPath projection root (e.g. {@code $doc[]}) — every descendant that matches this path
   *        contributes one row to the index.
   * @param fieldPaths ordered field sub-paths relative to {@code rootPath}; duplicates rejected by
   *        caller. Order matters — it's the leaf-page column order at scan time.
   * @param fieldTypes one type per {@code fieldPaths} entry.
   * @param indexDefNo stable id slot for this index in the resource's index catalogue.
   * @param dbType XML / JSON — the projection is shaped by the shredder, same as other indexes.
   */
  IndexDef(final Path<QNm> rootPath, final List<Path<QNm>> fieldPaths, final List<Type> fieldTypes,
      final int indexDefNo, final DbType dbType) {
    this(rootPath, fieldPaths, fieldTypes, indexDefNo, dbType, null);
  }

  IndexDef(final Path<QNm> rootPath, final List<Path<QNm>> fieldPaths, final List<Type> fieldTypes,
      final int indexDefNo, final DbType dbType, final @Nullable ProjectionSortedSpec sortedSpec) {
    if (fieldPaths.size() != fieldTypes.size()) {
      throw new IllegalArgumentException("projection field-path count (" + fieldPaths.size()
          + ") must match field-type count (" + fieldTypes.size() + ")");
    }
    type = IndexType.PROJECTION;
    this.paths.add(requireNonNull(rootPath));
    this.projectionFields.addAll(fieldPaths);
    this.projectionFieldTypes.addAll(fieldTypes);
    if (sortedSpec != null) {
      sortedSpec.validate(fieldPaths, fieldTypes);
    }
    this.projectionSortedSpec = sortedSpec;
    id = indexDefNo;
    this.dbType = dbType;
  }

  @Override
  public Node<?> materialize() throws DocumentException {
    final FragmentHelper tmp = new FragmentHelper();

    tmp.openElement(INDEX_TAG);
    tmp.attribute(TYPE_ATTRIBUTE, new Una(type.toString()));
    tmp.attribute(DB_TYPE_ATTRIBUTE, new Una(dbType.toString()));
    tmp.attribute(ID_ATTRIBUTE, new Una(Integer.toString(id)));

    if (type == IndexType.VALIDTIME) {
      tmp.attribute(VALID_TIME_FORMAT_ATTRIBUTE, new Una(validTimeFormat));
    }

    if (contentType != null) {
      tmp.attribute(CONTENT_TYPE_ATTRIBUTE, new Una(contentType.toString()));
    }

    if (unique) {
      tmp.attribute(UNIQUE_ATTRIBUTE, new Una(Boolean.toString(unique)));
    }

    if (type == IndexType.VECTOR) {
      tmp.attribute(DIMENSION_ATTRIBUTE, new Una(Integer.toString(dimension)));
      tmp.attribute(DISTANCE_TYPE_ATTRIBUTE, new Una(distanceType));
      tmp.attribute(HNSW_M_ATTRIBUTE, new Una(Integer.toString(hnswM)));
      tmp.attribute(HNSW_EF_CONSTRUCTION_ATTRIBUTE, new Una(Integer.toString(hnswEfConstruction)));
      tmp.attribute(HNSW_EF_SEARCH_ATTRIBUTE, new Una(Integer.toString(hnswEfSearch)));
    }

    if (!paths.isEmpty()) {
      for (final Path<QNm> path : paths) {
        tmp.openElement(PATH_TAG);
        materializePath(tmp, path);
        tmp.closeElement();
      }
    }

    if (type == IndexType.PROJECTION && !projectionFields.isEmpty()) {
      tmp.openElement(PROJECTION_FIELDS_TAG);
      for (int i = 0, n = projectionFields.size(); i < n; i++) {
        tmp.openElement(PROJECTION_FIELD_TAG);
        tmp.attribute(PROJECTION_FIELD_TYPE_ATTRIBUTE, new Una(projectionFieldTypes.get(i).toString()));
        materializePath(tmp, projectionFields.get(i));
        tmp.closeElement();
      }
      tmp.closeElement();
    }

    if (type == IndexType.PROJECTION && projectionSortedSpec != null) {
      tmp.openElement(PROJECTION_SORT_TAG);
      for (final int column : projectionSortedSpec.keyColumns()) {
        tmp.openElement(PROJECTION_SORT_KEY_TAG);
        tmp.attribute(PROJECTION_SORT_COLUMN_ATTRIBUTE, new Una(Integer.toString(column)));
        tmp.closeElement();
      }
      tmp.closeElement();
    }

    materializeNames(tmp, EXCLUDING_TAG, excluded);
    materializeNames(tmp, INCLUDING_TAG, included);
    //
    // if (indexStatistics != null) {
    // tmp.insert(indexStatistics.materialize());
    // }

    tmp.closeElement();
    return tmp.getRoot();
  }

  /**
   * Persist each filter QName as a name child with UTF-8/Base64 {@code uri}, {@code prefix}, and
   * {@code local} attributes. Separate components preserve URIs and names containing commas; Base64
   * protects XML-sensitive characters from serialization and attribute-normalization losses.
   */
  private static void materializeNames(final FragmentHelper fragment, final QNm tag, final Set<QNm> names) {
    if (names.isEmpty()) {
      return;
    }
    fragment.openElement(tag);
    for (final QNm name : names) {
      fragment.openElement(NAME_TAG);
      fragment.attribute(NAME_URI_ATTRIBUTE, new Una(encodeNameComponent(name.getNamespaceURI())));
      fragment.attribute(NAME_PREFIX_ATTRIBUTE, new Una(encodeNameComponent(name.getPrefix())));
      fragment.attribute(NAME_LOCAL_ATTRIBUTE, new Una(encodeNameComponent(name.getLocalName())));
      fragment.closeElement();
    }
    fragment.closeElement();
  }

  private static String encodeNameComponent(final String value) {
    return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * XML paths must retain axes and expanded names: lexical printing loses namespace URIs. Name
   * components use the same encoding as {@link #materializeNames} to avoid XML normalization losses.
   */
  private void materializePath(final FragmentHelper fragment, final Path<QNm> path) {
    if (dbType == DbType.JSON) {
      fragment.content(path.toString());
      return;
    }
    for (final Step<QNm> step : path.steps()) {
      fragment.openElement(PATH_STEP_TAG);
      fragment.attribute(PATH_AXIS_ATTRIBUTE, new Una(step.getAxis().name()));
      final QNm name = step.getValue();
      if (name != null) {
        fragment.attribute(NAME_URI_ATTRIBUTE, new Una(encodeNameComponent(name.getNamespaceURI())));
        fragment.attribute(NAME_PREFIX_ATTRIBUTE, new Una(encodeNameComponent(name.getPrefix())));
        fragment.attribute(NAME_LOCAL_ATTRIBUTE, new Una(encodeNameComponent(name.getLocalName())));
      }
      fragment.closeElement();
    }
  }

  /**
   * Reject legacy lexical XML paths rather than silently losing namespace constraints. JSON keeps its
   * printed-path contract; {@link #hasSameDefinition(IndexDef)} owns comparison semantics.
   */
  private Path<QNm> readPath(final Node<?> node) {
    if (dbType == DbType.JSON) {
      return Path.parse(node.getValue().stringValue(), PathParser.Type.JSON);
    }
    if (!node.getValue().stringValue().isBlank()) {
      throw new DocumentException("XML index paths require encoded axes and expanded names");
    }
    final Path<QNm> path = new Path<>();
    try (final Stream<? extends Node<?>> children = node.getChildren()) {
      Node<?> step;
      while ((step = children.next()) != null) {
        if (step.getName() == null && step.getValue().stringValue().isBlank()) {
          continue;
        }
        if (!PATH_STEP_TAG.equals(step.getName())) {
          throw new DocumentException("Expected XML path step but found '%s'", step.getName());
        }
        final Node<?> axisAttribute = step.getAttribute(PATH_AXIS_ATTRIBUTE);
        if (axisAttribute == null) {
          throw new DocumentException("Missing XML path axis");
        }
        final Axis axis;
        try {
          axis = Axis.valueOf(axisAttribute.getValue().stringValue());
        } catch (final IllegalArgumentException invalid) {
          throw new DocumentException(invalid, "Invalid XML path axis");
        }
        final QNm name = step.getAttribute(NAME_LOCAL_ATTRIBUTE) == null
            ? null
            : new QNm(readNameComponent(step, NAME_URI_ATTRIBUTE), readNameComponent(step, NAME_PREFIX_ATTRIBUTE),
                readNameComponent(step, NAME_LOCAL_ATTRIBUTE));
        switch (axis) {
          case CHILD -> path.child(name);
          case DESC -> path.descendant(name);
          case CHILD_ATTRIBUTE -> path.attribute(name);
          case DESC_ATTRIBUTE -> path.descendantAttribute(name);
          case PARENT -> path.parent();
          case SELF -> path.self();
          default -> throw new DocumentException("Unsupported XML path axis '%s'", axis);
        }
      }
    }
    return path;
  }

  private static String readNameComponent(final Node<?> name, final QNm attribute) {
    return new String(Base64.getDecoder().decode(name.getAttribute(attribute).getValue().stringValue()),
        StandardCharsets.UTF_8);
  }

  private static void readNames(final Node<?> parent, final Set<QNm> names) {
    try (final Stream<? extends Node<?>> children = parent.getChildren()) {
      Node<?> child;
      while ((child = children.next()) != null) {
        if (NAME_TAG.equals(child.getName())) {
          names.add(new QNm(readNameComponent(child, NAME_URI_ATTRIBUTE),
              readNameComponent(child, NAME_PREFIX_ATTRIBUTE), readNameComponent(child, NAME_LOCAL_ATTRIBUTE)));
        }
      }
    }
  }

  @Override
  public void init(final Node<?> root) throws DocumentException {
    final QNm name = root.getName();

    if (!name.equals(INDEX_TAG)) {
      throw new DocumentException("Expected tag '%s' but found '%s'", INDEX_TAG, name);
    }

    Node<?> attribute;

    attribute = root.getAttribute(ID_ATTRIBUTE);
    if (attribute != null) {
      id = Integer.parseInt(attribute.getValue().stringValue());
    }

    attribute = root.getAttribute(TYPE_ATTRIBUTE);
    if (attribute != null) {
      type = IndexType.valueOf(attribute.getValue().stringValue());
    }

    if (type == IndexType.VALIDTIME) {
      attribute = root.getAttribute(VALID_TIME_FORMAT_ATTRIBUTE);
      validTimeFormat = attribute == null ? "" : attribute.getValue().stringValue();
    }

    attribute = root.getAttribute(CONTENT_TYPE_ATTRIBUTE);
    if (attribute != null) {
      contentType = resolveType(attribute.getValue().stringValue());
    }

    attribute = root.getAttribute(UNIQUE_ATTRIBUTE);
    if (attribute != null) {
      unique = Boolean.parseBoolean(attribute.getValue().stringValue());
    }

    attribute = root.getAttribute(DB_TYPE_ATTRIBUTE);
    if (attribute != null) {
      dbType = DbType.ofString(attribute.getValue().stringValue())
                     .orElseThrow(() -> new DocumentException("Invalid db type"));
    }

    attribute = root.getAttribute(DIMENSION_ATTRIBUTE);
    if (attribute != null) {
      dimension = Integer.parseInt(attribute.getValue().stringValue());
    }

    attribute = root.getAttribute(DISTANCE_TYPE_ATTRIBUTE);
    if (attribute != null) {
      distanceType = attribute.getValue().stringValue();
    }

    attribute = root.getAttribute(HNSW_M_ATTRIBUTE);
    if (attribute != null) {
      hnswM = Integer.parseInt(attribute.getValue().stringValue());
    }

    attribute = root.getAttribute(HNSW_EF_CONSTRUCTION_ATTRIBUTE);
    if (attribute != null) {
      hnswEfConstruction = Integer.parseInt(attribute.getValue().stringValue());
    }

    attribute = root.getAttribute(HNSW_EF_SEARCH_ATTRIBUTE);
    if (attribute != null) {
      hnswEfSearch = Integer.parseInt(attribute.getValue().stringValue());
    }

    try (Stream<? extends Node<?>> children = root.getChildren()) {
      Node<?> child;
      while ((child = children.next()) != null) {
        // if (child.getName().equals(IndexStatistics.STATISTICS_TAG)) {
        // indexStatistics = new IndexStatistics();
        // indexStatistics.init(child);
        // } else {
        final QNm childName = child.getName();

        if (childName.equals(PATH_TAG)) {
          paths.add(readPath(child));
        } else if (childName.equals(INCLUDING_TAG)) {
          readNames(child, included);
        } else if (childName.equals(EXCLUDING_TAG)) {
          readNames(child, excluded);
        } else if (childName.equals(PROJECTION_FIELDS_TAG)) {
          try (Stream<? extends Node<?>> fieldNodes = child.getChildren()) {
            Node<?> fieldNode;
            while ((fieldNode = fieldNodes.next()) != null) {
              if (!fieldNode.getName().equals(PROJECTION_FIELD_TAG))
                continue;
              final Node<?> typeAttr = fieldNode.getAttribute(PROJECTION_FIELD_TYPE_ATTRIBUTE);
              final Type fieldType = typeAttr != null
                  ? resolveType(typeAttr.getValue().stringValue())
                  : Type.STR;
              projectionFields.add(readPath(fieldNode));
              projectionFieldTypes.add(fieldType);
            }
          }
        } else if (childName.equals(PROJECTION_SORT_TAG)) {
          if (projectionSortedSpec != null) {
            throw new DocumentException("Duplicate sorted projection declaration");
          }
          final List<Integer> keyColumns = new ArrayList<>();
          try (Stream<? extends Node<?>> sortNodes = child.getChildren()) {
            Node<?> sortNode;
            while ((sortNode = sortNodes.next()) != null) {
              final Node<?> columnAttribute = sortNode.getAttribute(PROJECTION_SORT_COLUMN_ATTRIBUTE);
              if (columnAttribute == null) {
                throw new DocumentException("Sorted projection entry has no column number");
              }
              final int column = Integer.parseInt(columnAttribute.getValue().stringValue());
              if (!sortNode.getName().equals(PROJECTION_SORT_KEY_TAG)) {
                throw new DocumentException("Unknown sorted projection entry: %s", sortNode.getName());
              }
              keyColumns.add(column);
            }
          }
          projectionSortedSpec = new ProjectionSortedSpec(keyColumns);
        }
        // }
      }
    }
    if (projectionSortedSpec != null) {
      if (type != IndexType.PROJECTION) {
        throw new DocumentException("Sorted view belongs only to a projection index");
      }
      try {
        projectionSortedSpec.validate(projectionFields, projectionFieldTypes);
      } catch (final IllegalArgumentException invalid) {
        throw new DocumentException(invalid, "Invalid sorted projection declaration: %s", invalid.getMessage());
      }
    }
  }

  private static Type resolveType(final String s) throws DocumentException {
    final QNm name = new QNm(Namespaces.XS_NSURI, Namespaces.XS_PREFIX, s.substring(Namespaces.XS_PREFIX.length() + 1));
    for (final Type type : Type.builtInTypes) {
      if (type.getName().getLocalName().equals(name.getLocalName())) {
        return type;
      }
    }
    throw new DocumentException("Unknown content type type: '%s'", name);
  }

  public boolean isNameIndex() {
    return type == IndexType.NAME;
  }

  public boolean isCasIndex() {
    return type == IndexType.CAS;
  }

  public boolean isPathIndex() {
    return type == IndexType.PATH;
  }

  public boolean isVectorIndex() {
    return type == IndexType.VECTOR;
  }

  public boolean isProjectionIndex() {
    return type == IndexType.PROJECTION;
  }

  public boolean isValidTimeIndex() {
    return type == IndexType.VALIDTIME;
  }

  /**
   * Ordered list of projection-index sub-field paths. Index {@code i} in the returned list matches
   * column {@code i} in every leaf page of the projection's HOT sub-tree. Only meaningful for
   * {@link IndexType#PROJECTION}.
   */
  public List<Path<QNm>> getProjectionFields() {
    return Collections.unmodifiableList(projectionFields);
  }

  /**
   * Value types for the projection's columns, index-aligned with {@link #getProjectionFields()}.
   * Drives the leaf-page column layout: {@code INR}/{@code LON} → packed {@code long[]}, {@code BOOL}
   * → bit array, {@code STR} → dict-id {@code int[]} plus per-leaf local dict.
   */
  public List<Type> getProjectionFieldTypes() {
    return Collections.unmodifiableList(projectionFieldTypes);
  }

  public @Nullable ProjectionSortedSpec getProjectionSortedSpec() {
    return projectionSortedSpec;
  }

  /**
   * For a projection index, the declared root path (e.g. {@code $doc[]}); every descendant matching
   * this path contributes a row. Returns {@code null} when the index isn't a projection or its root
   * hasn't been declared.
   */
  public Path<QNm> getProjectionRootPath() {
    if (type != IndexType.PROJECTION || paths.isEmpty())
      return null;
    return paths.iterator().next();
  }

  public int getDimension() {
    return dimension;
  }

  public String getDistanceType() {
    return distanceType;
  }

  public int getHnswM() {
    return hnswM;
  }

  public int getHnswEfConstruction() {
    return hnswEfConstruction;
  }

  public int getHnswEfSearch() {
    return hnswEfSearch;
  }

  public boolean isUnique() {
    return unique;
  }

  public int getID() {
    return id;
  }

  public IndexType getType() {
    return type;
  }

  public Set<Path<QNm>> getPaths() {
    return Collections.unmodifiableSet(paths);
  }

  public Set<QNm> getIncluded() {
    return Collections.unmodifiableSet(included);
  }

  public Set<QNm> getExcluded() {
    return Collections.unmodifiableSet(excluded);
  }

  @Override
  public String toString() {
    try {
      final ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SubtreePrinter.print(materialize(), new PrintStream(buf));
      return buf.toString();
    } catch (final DocumentException e) {
      return e.getMessage();
    }
  }

  public Type getContentType() {
    return contentType;
  }

  public boolean needsValidTimeRebuild() {
    return isValidTimeIndex() && !VALID_TIME_FORMAT.equals(validTimeFormat);
  }

  /**
   * Compare the complete persisted meaning of two index definitions.
   *
   * <p>
   * {@link #equals(Object)} deliberately identifies the catalogue slot by {@code (id,type)}. Creation
   * and listener binding need a stronger check: silently accepting the same physical slot with
   * different paths, filters, value types, projection columns, or vector parameters would make the
   * catalogue describe one index while the writer maintained another.
   * </p>
   *
   * <p>
   * XML paths persist exact axes and expanded names and are compared structurally. JSON paths are
   * compared in their PERSISTED form: {@link #materialize()} stores them as {@link Path#toString()}
   * and {@link #init(Node)} parses that text back, and the parser does not reproduce the internal
   * step representation for every spelling it accepts: a relative JSON name such as {@code foo}
   * parses with a CHILD step, prints as {@code ./foo}, and re-parses as CHILD_OBJECT_FIELD, so
   * {@link Path#equals(Object)} reports a definition as different from its own persisted copy. The
   * catalogue re-binds listeners from that copy on every commit; comparing the printed form is what
   * makes a definition equal to what the catalogue will hand back.
   * </p>
   *
   * @param other definition to compare
   * @return {@code true} only when every persisted semantic field is equal
   */
  public boolean hasSameDefinition(final IndexDef other) {
    requireNonNull(other);
    return id == other.id && type == other.type && dbType == other.dbType && unique == other.unique
        && (!isValidTimeIndex() || validTimeFormat.equals(other.validTimeFormat))
        && Objects.equals(contentType, other.contentType) && samePersistedPaths(paths, other.paths)
        && included.equals(other.included) && excluded.equals(other.excluded)
        && samePersistedPaths(projectionFields, other.projectionFields)
        && projectionFieldTypes.equals(other.projectionFieldTypes)
        && Objects.equals(projectionSortedSpec, other.projectionSortedSpec) && dimension == other.dimension
        && Objects.equals(distanceType, other.distanceType) && hnswM == other.hnswM
        && hnswEfConstruction == other.hnswEfConstruction && hnswEfSearch == other.hnswEfSearch;
  }

  /**
   * Set equality of paths by their persisted meaning; see {@link #hasSameDefinition(IndexDef)}.
   * Definition validation is a catalogue operation, never a per-record path, so the transient set is
   * acceptable here.
   */
  private boolean samePersistedPaths(final Set<Path<QNm>> left, final Set<Path<QNm>> right) {
    if (dbType == DbType.XML) {
      return left.equals(right);
    }
    final Set<String> leftPrinted = new HashSet<>(left.size() * 2);
    for (final Path<QNm> path : left) {
      leftPrinted.add(path.toString());
    }
    final Set<String> rightPrinted = new HashSet<>(right.size() * 2);
    for (final Path<QNm> path : right) {
      rightPrinted.add(path.toString());
    }
    return leftPrinted.equals(rightPrinted);
  }

  /** Positional equality of projection field paths by their persisted meaning. */
  private boolean samePersistedPaths(final List<Path<QNm>> left, final List<Path<QNm>> right) {
    if (dbType == DbType.XML) {
      return left.equals(right);
    }
    if (left.size() != right.size()) {
      return false;
    }
    for (int i = 0, n = left.size(); i < n; i++) {
      if (!left.get(i).toString().equals(right.get(i).toString())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = id;
    result = 31 * result + ((type == null)
        ? 0
        : type.hashCode());
    return result;
  }


  @Override
  public boolean equals(final @Nullable Object obj) {
    if (this == obj)
      return true;

    if (!(obj instanceof final IndexDef other))
      return false;

    return id == other.id && type == other.type;
  }
}
