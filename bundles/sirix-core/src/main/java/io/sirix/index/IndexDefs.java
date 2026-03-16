package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.page.PageConstants;

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
   * @param unique determine if it's unique
   * @param optType an optional type
   * @param paths the paths to index
   * @return a new {@link IndexDef} instance
   */
  public static IndexDef createCASIdxDef(final boolean unique, final Type optType, final Set<Path<QNm>> paths,
      final int indexDefNo, final IndexDef.DbType dbType) {
    final Type type = optType == null
        ? Type.STR
        : optType;
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
      case JSON ->
        new IndexDef(Set.of(), Set.of(), PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo, dbType);
      case XML ->
        new IndexDef(Set.of(), Set.of(), PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo, dbType);
    };
  }

  public static IndexDef createFilteredNameIdxDef(final Set<QNm> excluded, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return switch (dbType) {
      case JSON -> new IndexDef(Set.of(), excluded, PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo, dbType);
      case XML -> new IndexDef(Set.of(), excluded, PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo, dbType);
    };
  }

  public static IndexDef createSelectiveNameIdxDef(final Set<QNm> included, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return switch (dbType) {
      case JSON -> new IndexDef(included, Set.of(), PageConstants.JSON_NAME_INDEX_OFFSET + indexDefNo, dbType);
      case XML -> new IndexDef(included, Set.of(), PageConstants.XML_NAME_INDEX_OFFSET + indexDefNo, dbType);
    };
  }

  /**
   * Create a vector {@link IndexDef} with default HNSW parameters (m=16, efConstruction=200).
   *
   * @param dimension the vector dimension
   * @param distanceType the distance metric ("L2", "COSINE", "INNER_PRODUCT")
   * @param paths the paths to index
   * @param indexDefNo the index definition number
   * @param dbType the database type
   * @return a new vector {@link IndexDef} instance
   */
  public static IndexDef createVectorIdxDef(final int dimension, final String distanceType,
      final Set<Path<QNm>> paths, final int indexDefNo, final IndexDef.DbType dbType) {
    return new IndexDef(dimension, distanceType, paths, 16, 200, indexDefNo, dbType);
  }

  /**
   * Create a vector {@link IndexDef} with custom HNSW parameters.
   *
   * @param dimension the vector dimension
   * @param distanceType the distance metric ("L2", "COSINE", "INNER_PRODUCT")
   * @param paths the paths to index
   * @param hnswM the HNSW M parameter (max connections per layer)
   * @param hnswEfConstruction the HNSW efConstruction parameter (candidate list size during build)
   * @param indexDefNo the index definition number
   * @param dbType the database type
   * @return a new vector {@link IndexDef} instance
   */
  public static IndexDef createVectorIdxDef(final int dimension, final String distanceType,
      final Set<Path<QNm>> paths, final int hnswM, final int hnswEfConstruction,
      final int indexDefNo, final IndexDef.DbType dbType) {
    return new IndexDef(dimension, distanceType, paths, hnswM, hnswEfConstruction, indexDefNo, dbType);
  }

  /**
   * Create a vector {@link IndexDef} with custom HNSW parameters including efSearch.
   *
   * @param dimension the vector dimension
   * @param distanceType the distance metric ("L2", "COSINE", "INNER_PRODUCT")
   * @param paths the paths to index
   * @param hnswM the HNSW M parameter (max connections per layer)
   * @param hnswEfConstruction the HNSW efConstruction parameter (candidate list size during build)
   * @param hnswEfSearch the HNSW efSearch parameter (candidate list size during search)
   * @param indexDefNo the index definition number
   * @param dbType the database type
   * @return a new vector {@link IndexDef} instance
   */
  public static IndexDef createVectorIdxDef(final int dimension, final String distanceType,
      final Set<Path<QNm>> paths, final int hnswM, final int hnswEfConstruction,
      final int hnswEfSearch, final int indexDefNo, final IndexDef.DbType dbType) {
    return new IndexDef(dimension, distanceType, paths, hnswM, hnswEfConstruction,
        hnswEfSearch, indexDefNo, dbType);
  }
}
