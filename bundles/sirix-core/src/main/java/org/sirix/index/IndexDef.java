package org.sirix.index;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Una;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.node.parser.FragmentHelper;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.serialize.SubtreePrinter;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.Type;
import org.brackit.xquery.xdm.node.Node;

public final class IndexDef implements Materializable {
  private static final QNm EXCLUDING_TAG = new QNm("excluding");

  private static final QNm INCLUDING_TAG = new QNm("including");

  private static final QNm PATH_TAG = new QNm("path");

  private static final QNm UNIQUE_ATTRIBUTE = new QNm("unique");

  private static final QNm CONTENT_TYPE_ATTRIBUTE = new QNm("keyType");

  private static final QNm TYPE_ATTRIBUTE = new QNm("type");

  private static final QNm ID_ATTRIBUTE = new QNm("id");

  public static final QNm INDEX_TAG = new QNm("index");

  private IndexType mType;

  // unique flag (for CAS indexes)
  private boolean mUnique = false;

  // for CAS indexes
  private Type mContentType;

  // populated when index is built
  private int mID;

  private final Set<Path<QNm>> mPaths = new HashSet<>();

  private final Set<QNm> mExcluded = new HashSet<>();

  private final Set<QNm> mIncluded = new HashSet<>();

  public IndexDef() {}

  /**
   * Name index.
   */
  IndexDef(final Set<QNm> included, final Set<QNm> excluded, final int indexDefNo) {
    mType = IndexType.NAME;
    mIncluded.addAll(included);
    mExcluded.addAll(excluded);
    mID = indexDefNo;
  }

  /**
   * Path index.
   */
  IndexDef(final Set<Path<QNm>> paths, final int indexDefNo) {
    mType = IndexType.PATH;
    mPaths.addAll(paths);
    mID = indexDefNo;
  }

  /**
   * CAS index.
   */
  IndexDef(final Type contentType, final Set<Path<QNm>> paths, final boolean unique,
      final int indexDefNo) {
    mType = IndexType.CAS;
    mContentType = checkNotNull(contentType);
    mPaths.addAll(paths);
    mUnique = unique;
    mID = indexDefNo;
  }

  @Override
  public Node<?> materialize() throws DocumentException {
    final FragmentHelper tmp = new FragmentHelper();

    tmp.openElement(INDEX_TAG);
    tmp.attribute(TYPE_ATTRIBUTE, new Una(mType.toString()));
    tmp.attribute(ID_ATTRIBUTE, new Una(Integer.toString(mID)));

    if (mContentType != null) {
      tmp.attribute(CONTENT_TYPE_ATTRIBUTE, new Una(mContentType.toString()));
    }

    if (mUnique) {
      tmp.attribute(UNIQUE_ATTRIBUTE, new Una(Boolean.toString(mUnique)));
    }

    if (mPaths != null && !mPaths.isEmpty()) {
      for (final Path<QNm> path : mPaths) {
        tmp.openElement(PATH_TAG);
        tmp.content(path.toString()); // TODO
        tmp.closeElement();
      }
    }

    if (!mExcluded.isEmpty()) {
      tmp.openElement(EXCLUDING_TAG);

      final StringBuilder buf = new StringBuilder();
      for (final QNm s : mExcluded) {
        buf.append(s + ",");
      }
      // remove trailing ","
      buf.deleteCharAt(buf.length() - 1);
      tmp.content(buf.toString());
      tmp.closeElement();
    }

    if (!mIncluded.isEmpty()) {
      tmp.openElement(INCLUDING_TAG);

      final StringBuilder buf = new StringBuilder();
      for (final QNm incl : mIncluded) {
        buf.append(incl + ",");
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
      mID = Integer.valueOf(attribute.getValue().stringValue());
    }

    attribute = root.getAttribute(TYPE_ATTRIBUTE);
    if (attribute != null) {
      mType = (IndexType.valueOf(attribute.getValue().stringValue()));
    }

    attribute = root.getAttribute(CONTENT_TYPE_ATTRIBUTE);
    if (attribute != null) {
      mContentType = (resolveType(attribute.getValue().stringValue()));
    }

    attribute = root.getAttribute(UNIQUE_ATTRIBUTE);
    if (attribute != null) {
      mUnique = (Boolean.valueOf(attribute.getValue().stringValue()));
    }

    final Stream<? extends Node<?>> children = root.getChildren();

    try {
      Node<?> child;
      while ((child = children.next()) != null) {
        // if (child.getName().equals(IndexStatistics.STATISTICS_TAG)) {
        // indexStatistics = new IndexStatistics();
        // indexStatistics.init(child);
        // } else {
        final QNm childName = child.getName();
        final String value = child.getValue().stringValue();

        if (childName.equals(PATH_TAG)) {
          final String path = value;
          mPaths.add(Path.parse(path));
        } else if (childName.equals(INCLUDING_TAG)) {
          for (final String s : value.split(",")) {
            if (s.length() > 0) {
              mIncluded.add(new QNm(s));
              // String includeString = s;
              // String[] tmp = includeString.split("@");
              // included.put(new QNm(tmp[0]),
              // Cluster.valueOf(tmp[1]));
            }
          }
        } else if (childName.equals(EXCLUDING_TAG)) {
          for (final String s : value.split(",")) {
            if (s.length() > 0)
              mExcluded.add(new QNm(s));
          }
        }
        // }
      }
    } finally {
      children.close();
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
    return mType == IndexType.NAME;
  }

  public boolean isCasIndex() {
    return mType == IndexType.CAS;
  }

  public boolean isPathIndex() {
    return mType == IndexType.PATH;
  }

  public boolean isUnique() {
    return mUnique;
  }

  public int getID() {
    return mID;
  }

  public IndexType getType() {
    return mType;
  }

  public Set<Path<QNm>> getPaths() {
    return Collections.unmodifiableSet(mPaths);
  }

  public Set<QNm> getIncluded() {
    return Collections.unmodifiableSet(mIncluded);
  }

  public Set<QNm> getExcluded() {
    return Collections.unmodifiableSet(mExcluded);
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
    return mContentType;
  }

  @Override
  public int hashCode() {
    int result = mID;
    result = 31 * result + ((mType == null)
        ? 0
        : mType.hashCode());
    return result;
  }


  @Override
  public boolean equals(final @Nullable Object obj) {
    if (this == obj)
      return true;

    if (!(obj instanceof IndexDef))
      return false;

    final IndexDef other = (IndexDef) obj;
    return mID == other.mID && mType == other.mType;
  }
}
