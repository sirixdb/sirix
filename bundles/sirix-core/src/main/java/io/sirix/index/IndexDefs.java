package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.page.PageConstants;
import io.sirix.settings.Constants;

import java.util.List;
import java.util.Objects;
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
    return new IndexDef(Set.of(), Set.of(), physicalNameIndexId(indexDefNo, dbType), dbType);
  }

  public static IndexDef createFilteredNameIdxDef(final Set<QNm> excluded, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return new IndexDef(Set.of(), excluded, physicalNameIndexId(indexDefNo, dbType), dbType);
  }

  public static IndexDef createSelectiveNameIdxDef(final Set<QNm> included, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return new IndexDef(included, Set.of(), physicalNameIndexId(indexDefNo, dbType), dbType);
  }

  /**
   * Map a logical NAME definition number onto its non-overlapping physical
   * {@link io.sirix.page.NamePage} reference slot.
   */
  private static int physicalNameIndexId(final int indexDefNo, final IndexDef.DbType dbType) {
    Objects.requireNonNull(dbType, "dbType must not be null");
    if (indexDefNo < 0) {
      throw new IllegalArgumentException("NAME index definition number must be non-negative: " + indexDefNo);
    }

    final int base = switch (dbType) {
      case JSON -> PageConstants.JSON_NAME_INDEX_OFFSET;
      case XML -> PageConstants.XML_NAME_INDEX_OFFSET;
    };
    if (indexDefNo >= Constants.INP_REFERENCE_COUNT - base) {
      throw new IllegalArgumentException(
          "NAME index definition number " + indexDefNo + " maps outside the NamePage reference space");
    }
    return base + indexDefNo;
  }

  /**
   * Convert a validated physical {@link io.sirix.page.NamePage} secondary-index slot back to the
   * logical definition number accepted by the NAME factory methods.
   *
   * @param physicalIndexId physical NamePage reference slot
   * @param dbType database type defining the reserved NamePage prefix
   * @return logical NAME index definition number
   */
  public static int logicalNameIndexDefNoForPhysicalSlot(final int physicalIndexId, final IndexDef.DbType dbType) {
    Objects.requireNonNull(dbType, "dbType must not be null");
    final int base = switch (dbType) {
      case JSON -> PageConstants.JSON_NAME_INDEX_OFFSET;
      case XML -> PageConstants.XML_NAME_INDEX_OFFSET;
    };
    if (physicalIndexId < base || physicalIndexId >= Constants.INP_REFERENCE_COUNT) {
      throw new IllegalArgumentException("Physical NAME index slot " + physicalIndexId + " is outside the secondary "
          + dbType + " NamePage range [" + base + ", " + Constants.INP_REFERENCE_COUNT + ")");
    }
    return physicalIndexId - base;
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
  public static IndexDef createVectorIdxDef(final int dimension, final String distanceType, final Set<Path<QNm>> paths,
      final int indexDefNo, final IndexDef.DbType dbType) {
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
  public static IndexDef createVectorIdxDef(final int dimension, final String distanceType, final Set<Path<QNm>> paths,
      final int hnswM, final int hnswEfConstruction, final int indexDefNo, final IndexDef.DbType dbType) {
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
  public static IndexDef createVectorIdxDef(final int dimension, final String distanceType, final Set<Path<QNm>> paths,
      final int hnswM, final int hnswEfConstruction, final int hnswEfSearch, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return new IndexDef(dimension, distanceType, paths, hnswM, hnswEfConstruction, hnswEfSearch, indexDefNo, dbType);
  }

  /**
   * Create a projection {@link IndexDef}. Rows materialised at query time correspond one-to-one with
   * the records matching {@code rootPath}, and each row carries the declared field columns in
   * {@code fieldPaths} order — HOT leaf pages are laid out as parallel primitive arrays (one per
   * column + a {@code recordKey} column), enabling SIMD-friendly multi-field filter scans without the
   * OBJECT_KEY indirection the generic predicate path pays.
   *
   * @param rootPath projection root (e.g. {@code $doc[]})
   * @param fieldPaths ordered sub-field paths; order dictates column layout
   * @param fieldTypes per-field value type (index-aligned with {@code fieldPaths})
   * @param indexDefNo stable id slot in the resource's index catalogue
   * @param dbType XML / JSON
   */
  public static IndexDef createProjectionIdxDef(final Path<QNm> rootPath, final List<Path<QNm>> fieldPaths,
      final List<Type> fieldTypes, final int indexDefNo, final IndexDef.DbType dbType) {
    return new IndexDef(rootPath, fieldPaths, fieldTypes, indexDefNo, dbType);
  }

  /**
   * Create a valid-time (bitemporal) interval {@link IndexDef}. The index registers each record
   * OBJECT's {@code [validFrom, validTo]} interval in a persistent Relational-Interval-Tree for
   * output-sensitive stabbing queries; the valid-time field names are read from the resource's
   * {@link io.sirix.access.ValidTimeConfig} at build/maintain time.
   *
   * @param paths the two indexed valid-time paths (e.g. {@code /[]/validFrom}, {@code /[]/validTo})
   * @param indexDefNo stable id slot in the resource's index catalogue
   * @param dbType XML / JSON
   * @return a new valid-time {@link IndexDef} instance
   */
  public static IndexDef createValidTimeIdxDef(final Set<Path<QNm>> paths, final int indexDefNo,
      final IndexDef.DbType dbType) {
    return new IndexDef(paths, indexDefNo, dbType, true);
  }
}
