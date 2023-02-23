package org.sirix.index;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Una;
import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.jdm.Stream;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.jdm.node.Node;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.node.parser.FragmentHelper;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathParser;
import org.brackit.xquery.util.serialize.SubtreePrinter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public final class IndexDef implements Materializable {
  private static final QNm DB_TYPE_ATTRIBUTE = new QNm("dbType");

  private static final QNm EXCLUDING_TAG = new QNm("excluding");

  private static final QNm INCLUDING_TAG = new QNm("including");

  private static final QNm PATH_TAG = new QNm("path");

  private static final QNm UNIQUE_ATTRIBUTE = new QNm("unique");

  private static final QNm CONTENT_TYPE_ATTRIBUTE = new QNm("keyType");

  private static final QNm TYPE_ATTRIBUTE = new QNm("type");

  private static final QNm ID_ATTRIBUTE = new QNm("id");

  public static final QNm INDEX_TAG = new QNm("index");

  private DbType dbType;

  private IndexType type;

  // unique flag (for CAS indexes)
  private boolean unique = false;

  // for CAS indexes
  private Type contentType;

  // populated when index is built
  private int id;

  public enum DbType {
    XML,

    JSON;

    public static Optional<DbType> ofString(String type) {
      return Arrays.stream(DbType.values())
                   .filter(env -> env.name().equalsIgnoreCase(type))
                   .findFirst();
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
  IndexDef(final Type contentType, final Set<Path<QNm>> paths, final boolean unique,
      final int indexDefNo, final DbType dbType) {
    type = IndexType.CAS;
    this.contentType = checkNotNull(contentType);
    this.paths.addAll(paths);
    this.unique = unique;
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

    if (contentType != null) {
      tmp.attribute(CONTENT_TYPE_ATTRIBUTE, new Una(contentType.toString()));
    }

    if (unique) {
      tmp.attribute(UNIQUE_ATTRIBUTE, new Una(Boolean.toString(unique)));
    }

    if (!paths.isEmpty()) {
      for (final Path<QNm> path : paths) {
        tmp.openElement(PATH_TAG);
        tmp.content(path.toString()); // TODO
        tmp.closeElement();
      }
    }

    if (!excluded.isEmpty()) {
      tmp.openElement(EXCLUDING_TAG);

      final StringBuilder buf = new StringBuilder();
      for (final QNm s : excluded) {
        buf.append(s).append(",");
      }
      // remove trailing ","
      buf.deleteCharAt(buf.length() - 1);
      tmp.content(buf.toString());
      tmp.closeElement();
    }

    if (!included.isEmpty()) {
      tmp.openElement(INCLUDING_TAG);

      final StringBuilder buf = new StringBuilder();
      for (final QNm incl : included) {
        buf.append(incl).append(",");
      }
      // remove trailing ","
      buf.deleteCharAt(buf.length() - 1);
      tmp.content(buf.toString());
      tmp.closeElement();
    }
    //
    // if (indexStatistics != null) {
    // tmp.insert(indexStatistics.materialize());
    // }

    tmp.closeElement();
    return tmp.getRoot();
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
      dbType = DbType.ofString(attribute.getValue().stringValue()).orElseThrow(() -> new DocumentException("Invalid db type"));
    }

    try (Stream<? extends Node<?>> children = root.getChildren()) {
      Node<?> child;
      while ((child = children.next()) != null) {
        // if (child.getName().equals(IndexStatistics.STATISTICS_TAG)) {
        // indexStatistics = new IndexStatistics();
        // indexStatistics.init(child);
        // } else {
        final QNm childName = child.getName();
        final String value = child.getValue().stringValue();

        if (childName.equals(PATH_TAG)) {
          paths.add(Path.parse(value, dbType == DbType.JSON ? PathParser.Type.JSON : PathParser.Type.XML));
        } else if (childName.equals(INCLUDING_TAG)) {
          for (final String s : value.split(",")) {
            if (s.length() > 0) {
              included.add(new QNm(s));
              // String includeString = s;
              // String[] tmp = includeString.split("@");
              // included.put(new QNm(tmp[0]),
              // Cluster.valueOf(tmp[1]));
            }
          }
        } else if (childName.equals(EXCLUDING_TAG)) {
          for (final String s : value.split(",")) {
            if (s.length() > 0)
              excluded.add(new QNm(s));
          }
        }
        // }
      }
    }
  }

  private static Type resolveType(final String s) throws DocumentException {
    final QNm name = new QNm(Namespaces.XS_NSURI, Namespaces.XS_PREFIX,
        s.substring(Namespaces.XS_PREFIX.length() + 1));
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
