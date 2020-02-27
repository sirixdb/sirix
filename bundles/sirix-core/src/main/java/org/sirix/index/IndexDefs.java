package org.sirix.index;

import com.google.common.collect.ImmutableSet;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Type;
import org.sirix.page.PageConstants;

import java.util.Set;

/**
 * {@link IndexDef} factory.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class IndexDefs {

  /**
   * Private constructor.
   */
  private IndexDefs() {
    throw new AssertionError("May never be instantiated!");
  }

  /**
   * Create a CAS {@link IndexDef} instance.
   *
   * @param unique determine if it's unique
   * @param optType an optional type
   * @param paths the paths to index
   * @return a new {@link IndexDef} instance
   */
  public static IndexDef createCASIdxDef(final boolean unique, final Type optType, final Set<Path<QNm>> paths,
      final int indexDefNo) {
    final Type type = optType == null ? Type.STR : optType;
    return new IndexDef(type, paths, unique, indexDefNo);
  }

  /**
   * Create a path {@link IndexDef}.
   *
   * @param paths the paths to index
   * @return a new path {@link IndexDef} instance
   */
  public static IndexDef createPathIdxDef(final Set<Path<QNm>> paths, final int indexDefNo) {
    return new IndexDef(paths, indexDefNo);
  }

  public enum NameIndexType {
    JSON,

    XML
  }

  public static IndexDef createNameIdxDef(final int indexDefNo, final NameIndexType type) {
    switch (type) {
      case JSON:
        return new IndexDef(ImmutableSet.of(), ImmutableSet.of(), PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo);
      case XML:
        return new IndexDef(ImmutableSet.of(), ImmutableSet.of(), PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo);
      default:
        throw new IllegalStateException("Type " + type + " not known.");
    }
  }

  public static IndexDef createFilteredNameIdxDef(final Set<QNm> excluded, final int indexDefNo,
      final NameIndexType type) {
    switch (type) {
      case JSON:
        return new IndexDef(ImmutableSet.of(), excluded, PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo);
      case XML:
        return new IndexDef(ImmutableSet.of(), excluded, PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo);
      default:
        throw new IllegalStateException("Type " + type + " not known.");
    }
  }

  public static IndexDef createSelectiveNameIdxDef(final Set<QNm> included, final int indexDefNo,
      final NameIndexType type) {
    switch (type) {
      case JSON:
        return new IndexDef(included, ImmutableSet.of(), PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo);
      case XML:
        return new IndexDef(included, ImmutableSet.of(), PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo);
      default:
        throw new IllegalStateException("Type " + type + " not known.");
    }
  }
}
