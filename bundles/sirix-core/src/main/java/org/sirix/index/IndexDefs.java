package org.sirix.index;

import com.google.common.collect.ImmutableSet;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.util.path.Path;
import org.sirix.page.PageConstants;

import java.util.Set;

/**
 * {@link IndexDef} factory.
 *
 * @author Johannes Lichtenberger
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
   * @param unique  determine if it's unique
   * @param optType an optional type
   * @param paths   the paths to index
   * @return a new {@link IndexDef} instance
   */
  public static IndexDef createCASIdxDef(final boolean unique, final Type optType, final Set<Path<QNm>> paths,
      final int indexDefNo, final IndexDef.DbType dbType) {
    final Type type = optType == null ? Type.STR : optType;
    return new IndexDef(type, paths, unique, indexDefNo, dbType);
  }

  /**
   * Create a path {@link IndexDef}.
   *
   * @param paths the paths to index
   * @return a new path {@link IndexDef} instance
   */
  public static IndexDef createPathIdxDef(final Set<Path<QNm>> paths, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return new IndexDef(paths, indexDefNo, dbType);
  }

  public static IndexDef createNameIdxDef(final int indexDefNo, final IndexDef.DbType dbType) {
    return switch (dbType) {
      case JSON -> new IndexDef(ImmutableSet.of(),
                                ImmutableSet.of(),
                                PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo,
                                dbType);
      case XML -> new IndexDef(ImmutableSet.of(),
                               ImmutableSet.of(),
                               PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo,
                               dbType);
    };
  }

  public static IndexDef createFilteredNameIdxDef(final Set<QNm> excluded, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return switch (dbType) {
      case JSON -> new IndexDef(ImmutableSet.of(), excluded, PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo, dbType);
      case XML -> new IndexDef(ImmutableSet.of(), excluded, PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo, dbType);
    };
  }

  public static IndexDef createSelectiveNameIdxDef(final Set<QNm> included, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return switch (dbType) {
      case JSON -> new IndexDef(included, ImmutableSet.of(), PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo, dbType);
      case XML -> new IndexDef(included, ImmutableSet.of(), PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo, dbType);
    };
  }
}
