package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.node.parser.FragmentHelper;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import org.jspecify.annotations.Nullable;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static io.sirix.utils.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Thread-safe index definition container.
 * <p>
 * Uses {@link CopyOnWriteArraySet} for lock-free read operations since index definitions are rarely
 * modified but frequently queried during index lookups. This eliminates synchronization overhead on
 * hot read paths.
 * </p>
 *
 * @author Karsten Schmidt
 * @author Sebastian Baechle
 */
public final class Indexes implements Materializable {
  public static final QNm INDEXES_TAG = new QNm("indexes");

  /**
   * Thread-safe set for index definitions. CopyOnWriteArraySet provides lock-free reads with
   * copy-on-write semantics for modifications - ideal for read-heavy, write-rare workloads like index
   * metadata.
   */
  private final Set<IndexDef> indexes;

  /**
   * Tracks whether index definitions have been mutated since last serialization. Used to skip
   * redundant index XML writes during intermediate auto-commits.
   */
  private volatile boolean dirty;

  /** {@link #catalogueRevision()} while these definitions have no catalogue file. */
  public static final int NO_CATALOGUE_FILE = -1;

  private static final IndexDef[] NO_DEFINITIONS = new IndexDef[0];

  /**
   * Revision of the {@code indexes/<revision>.xml} file these definitions were loaded from or last
   * serialized to, or {@link #NO_CATALOGUE_FILE}.
   */
  private volatile int catalogueRevision = NO_CATALOGUE_FILE;

  /** The definitions as they are in that file, so a commit can tell whether anything changed. */
  private volatile IndexDef[] persisted = NO_DEFINITIONS;

  public Indexes() {
    indexes = new CopyOnWriteArraySet<>();
  }

  /**
   * Returns a snapshot of all index definitions. Thread-safe without synchronization due to
   * CopyOnWriteArraySet.
   */
  public Set<IndexDef> getIndexDefs() {
    return new HashSet<>(indexes);
  }

  /** The definitions in their catalogued order (the order they were created or loaded in). */
  public List<IndexDef> getIndexDefsInOrder() {
    return new ArrayList<>(indexes);
  }

  public boolean isEmpty() {
    return indexes.isEmpty();
  }

  /**
   * @return the revision of the catalogue file these definitions are persisted in, or
   *         {@link #NO_CATALOGUE_FILE}
   */
  public int catalogueRevision() {
    return catalogueRevision;
  }

  /**
   * Replaces the definitions with the ones of the catalogue file {@code catalogueRevision}, as loaded
   * from it (or from a cache of its parse). Loading is not a mutation: the definitions are not dirty
   * and do not differ from what is persisted.
   *
   * @param catalogueRevision the revision of the file, or {@link #NO_CATALOGUE_FILE}
   * @param definitions the file's definitions in catalogued order
   */
  public void initFrom(final int catalogueRevision, final List<IndexDef> definitions) {
    requireNonNull(definitions);
    checkArgument(catalogueRevision >= NO_CATALOGUE_FILE, "catalogueRevision must be >= -1!");
    indexes.clear();
    for (final IndexDef definition : definitions) {
      indexes.add(requireNonNull(definition).copyForCatalogue());
    }
    this.catalogueRevision = catalogueRevision;
    persisted = indexes.toArray(NO_DEFINITIONS);
    dirty = false;
  }

  /**
   * Records that the current definitions were serialized to the catalogue file of {@code revision}.
   *
   * @param revision the revision whose catalogue file now holds exactly these definitions
   */
  public void markPersisted(final int revision) {
    checkArgument(revision >= 0, "revision must be >= 0!");
    persisted = indexes.toArray(NO_DEFINITIONS);
    catalogueRevision = revision;
    clearDirty();
  }

  /**
   * Whether the definitions differ from the ones in the catalogue file they were loaded from or last
   * serialized to. A commit serializes a catalogue only when this is {@code true} (or when it has to
   * re-publish the represented catalogue after a revert), so an unchanged catalogue costs a commit
   * neither a file nor an fsync.
   *
   * @return {@code true} when a commit has to serialize these definitions
   */
  public boolean differsFromPersisted() {
    final IndexDef[] snapshot = persisted;
    if (snapshot.length != indexes.size()) {
      return true;
    }
    for (final IndexDef definition : indexes) {
      if (definition.isNumericCoverageDirty()) {
        return true;
      }
      boolean found = false;
      for (final IndexDef persistedDefinition : snapshot) {
        if (persistedDefinition == definition || persistedDefinition.hasSameDefinition(definition)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets an index definition by index number and type. Thread-safe without synchronization due to
   * CopyOnWriteArraySet.
   */
  public IndexDef getIndexDef(final int indexNo, final IndexType type) {
    checkArgument(indexNo >= 0, "indexNo must be >= 0!");
    for (final IndexDef sid : indexes) {
      if (sid.getID() == indexNo && sid.getType() == type) {
        return sid;
      }
    }
    return null;
  }

  /**
   * Initializes indexes from persisted XML representation. Thread-safe: CopyOnWriteArraySet handles
   * concurrent modifications.
   */
  @Override
  public void init(final Node<?> root) throws DocumentException {
    final QNm name = root.getName();
    if (!INDEXES_TAG.equals(name)) {
      throw new DocumentException("Expected tag '%s' but found '%s'", INDEXES_TAG, name);
    }

    // A controller is cached by prospective write revision and can therefore be reused after a
    // rollback. Parse into a detached set first, then replace the cache entry's catalogue in one
    // cold-path publication. Additive initialization would retain definitions created only in the
    // aborted transaction and rebind listeners for index trees that were rolled back. The persisted
    // order is kept: "the first catalogued definition" of a shape must not change across a reopen.
    final Set<IndexDef> restoredIndexes = new LinkedHashSet<>();
    try (Stream<? extends Node<?>> children = root.getChildren()) {
      Node<?> child;
      while ((child = children.next()) != null) {
        QNm childName = child.getName();

        if (!childName.equals(IndexDef.INDEX_TAG)) {
          throw new DocumentException("Expected tag '%s' but found '%s'", IndexDef.INDEX_TAG, childName);
        }

        final Node<?> dbTypeAttrNode = child.getAttribute(new QNm("dbType"));

        final var dbType = IndexDef.DbType.ofString(dbTypeAttrNode.atomize().asStr().toString());

        final IndexDef indexDefinition =
            new IndexDef(dbType.orElseThrow(() -> new DocumentException("DB type not found.")));
        indexDefinition.init(child);
        restoredIndexes.add(indexDefinition);
      }
    }

    indexes.clear();
    indexes.addAll(restoredIndexes);
    persisted = restoredIndexes.toArray(NO_DEFINITIONS);
    // Loading from disk is not a mutation — clear dirty flag.
    dirty = false;
  }

  /** Reset a cached controller to the persisted empty-catalogue state. */
  public void reset() {
    indexes.clear();
    catalogueRevision = NO_CATALOGUE_FILE;
    persisted = NO_DEFINITIONS;
    dirty = false;
  }

  /**
   * Adopt successor catalogue membership, including drops, and inherited numeric coverage. Membership
   * alone is not a new mutation; coverage changes retain their dirty state for persistence. Matching
   * definitions keep the catalogue instance used by bound listeners, while copied definitions isolate
   * mutable evidence from the predecessor's catalogue.
   *
   * @param definitions the predecessor's authoritative definitions
   * @throws NullPointerException if {@code definitions} or an added definition is null
   */
  @SuppressWarnings("ReferenceEquality") // Identity determines whether listener-bound evidence needs isolation.
  public void replaceWith(final Set<IndexDef> definitions) {
    requireNonNull(definitions);
    indexes.retainAll(definitions);
    for (final IndexDef definition : definitions) {
      requireNonNull(definition);
      final IndexDef current = getIndexDef(definition.getID(), definition.getType());
      if (current == null || !current.hasSameDefinition(definition)
          || (current == definition && definition.isCasIndex() && definition.getContentType().isNumeric())) {
        if (current != null) {
          indexes.remove(current);
        }
        indexes.add(definition.copyForCatalogue());
      } else {
        current.adoptNumericCoverage(definition);
      }
    }
    dirty = false;
  }

  /**
   * Materializes indexes to XML representation. Thread-safe: CopyOnWriteArraySet provides consistent
   * snapshot for iteration.
   */
  @Override
  public Node<?> materialize() throws DocumentException {
    FragmentHelper helper = new FragmentHelper();
    helper.openElement(INDEXES_TAG);

    for (IndexDef idxDef : indexes) {
      helper.insert(idxDef.materialize());
    }

    helper.closeElement();
    return helper.getRoot();
  }

  /**
   * Returns whether index definitions have been mutated since last serialization or init.
   */
  public boolean isDirty() {
    if (dirty) {
      return true;
    }
    for (final IndexDef definition : indexes) {
      if (definition.isNumericCoverageDirty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Clears the dirty flag after successful serialization.
   */
  public void clearDirty() {
    dirty = false;
    for (final IndexDef definition : indexes) {
      definition.clearNumericCoverageDirty();
    }
  }

  /**
   * Adds an index definition. Thread-safe: CopyOnWriteArraySet handles concurrent modifications.
   */
  public void add(IndexDef indexDefinition) {
    if (indexes.add(indexDefinition)) {
      dirty = true;
    }
  }

  /**
   * Removes an index definition by ID. Thread-safe: CopyOnWriteArraySet handles concurrent
   * modifications.
   *
   * <p>
   * <b>Note:</b> index IDs are only unique WITHIN a type (each {@code create-*-index} numbers its own
   * type from 0), so this removes EVERY definition with the given id across ALL types. To drop a
   * specific index, prefer {@link #removeIndex(IndexDef)} which matches on (id, type).
   * </p>
   */
  public void removeIndex(final int indexID) {
    checkArgument(indexID >= 0, "indexID must be >= 0!");
    if (indexes.removeIf(indexDef -> indexDef.getID() == indexID)) {
      dirty = true;
    }
  }

  /**
   * Removes the given index definition (matched on (id, type) via {@link IndexDef#equals}). Only the
   * matching definition is removed; indexes of other types that happen to share the same id survive.
   * Thread-safe: CopyOnWriteArraySet handles concurrent modifications.
   */
  public void removeIndex(final IndexDef indexDef) {
    requireNonNull(indexDef);
    if (indexes.remove(indexDef)) {
      dirty = true;
    }
  }

  public Optional<IndexDef> findPathIndex(final Path<QNm> path) throws DocumentException {
    requireNonNull(path);
    try {
      for (final IndexDef index : indexes) {
        if (index.isPathIndex() && checkIfAPathMatches(path, index)) {
          return Optional.of(index);
        }
      }
      return Optional.empty();
    } catch (PathException e) {
      throw new DocumentException(e);
    }
  }

  private boolean checkIfAPathMatches(Path<QNm> path, IndexDef index) {
    if (index.getPaths().isEmpty()) {
      return true;
    }

    for (final Path<QNm> indexedPath : index.getPaths()) {
      if (indexedPath.matches(path)) {
        return true;
      }
    }
    return false;
  }

  public Optional<IndexDef> findCASIndex(final Path<QNm> path, final Type type) throws DocumentException {
    requireNonNull(path);
    try {
      for (final IndexDef index : indexes) {
        if (index.isCasIndex() && index.getContentType().equals(type) && checkIfAPathMatches(path, index)) {
          return Optional.of(index);
        }
      }
      return Optional.empty();
    } catch (PathException e) {
      throw new DocumentException(e);
    }
  }

  public Optional<IndexDef> findNameIndex(final QNm... names) throws DocumentException {
    requireNonNull(names);
    out: for (final IndexDef index : indexes) {
      if (index.isNameIndex()) {
        final Set<QNm> incl = index.getIncluded();
        final Set<QNm> excl = index.getExcluded();
        if (names.length == 0 && incl.isEmpty() && excl.isEmpty()) {
          // Require generic name index
          return Optional.of(index);
        }

        for (final QNm name : names) {
          if (!incl.isEmpty() && !incl.contains(name) || !excl.isEmpty() && excl.contains(name)) {
            continue out;
          }
        }
        return Optional.of(index);
      }
    }
    return Optional.empty();
  }

  /**
   * As {@link #findProjectionIndex(Path, List, List)}, additionally requiring exactly the given
   * sorted-view declaration: {@code jn:create-projection-index} refines a projection's identity by
   * its sort columns when they are given.
   */
  public Optional<IndexDef> findProjectionIndex(final Path<QNm> rootPath, final List<Path<QNm>> fieldPaths,
      final List<Type> fieldTypesOrNull, final ProjectionSortedSpec sortedSpec) {
    requireNonNull(rootPath);
    requireNonNull(fieldPaths);
    requireNonNull(sortedSpec);
    for (final IndexDef index : indexes) {
      if (sortedSpec.equals(index.getProjectionSortedSpec())
          && sameProjectionShape(index, rootPath, fieldPaths, fieldTypesOrNull)) {
        return Optional.of(index);
      }
    }
    return Optional.empty();
  }

  /**
   * Find a PROJECTION index by its shape: record-set root path, ordered field paths, and — when
   * {@code fieldTypesOrNull} is given — ordered declared types. Path comparison uses the parsed
   * paths' canonical form, matching the identity rule of {@code jn:create-projection-index} (sits
   * beside {@link #findPathIndex}/{@link #findCASIndex}/{@link #findNameIndex} as the projection
   * family's finder). A sorted-view declaration is not part of this key: the first catalogued
   * projection of the shape is returned, whether or not it declares a sorted view.
   */
  public Optional<IndexDef> findProjectionIndex(final Path<QNm> rootPath, final List<Path<QNm>> fieldPaths,
      final List<Type> fieldTypesOrNull) {
    requireNonNull(rootPath);
    requireNonNull(fieldPaths);
    for (final IndexDef index : indexes) {
      if (sameProjectionShape(index, rootPath, fieldPaths, fieldTypesOrNull)) {
        return Optional.of(index);
      }
    }
    return Optional.empty();
  }

  private static boolean sameProjectionShape(final IndexDef index, final Path<QNm> rootPath,
      final List<Path<QNm>> fieldPaths, final @Nullable List<Type> fieldTypesOrNull) {
    if (!index.isProjectionIndex() || !rootPath.toString().equals(index.getProjectionRootPath().toString())) {
      return false;
    }
    final List<Path<QNm>> indexedFields = index.getProjectionFields();
    if (indexedFields.size() != fieldPaths.size()
        || fieldTypesOrNull != null && !index.getProjectionFieldTypes().equals(fieldTypesOrNull)) {
      return false;
    }
    for (int i = 0; i < indexedFields.size(); i++) {
      if (!indexedFields.get(i).toString().equals(fieldPaths.get(i).toString())) {
        return false;
      }
    }
    return true;
  }

  public int getNrOfIndexDefsWithType(final IndexType type) {
    requireNonNull(type);
    int nr = 0;
    for (final IndexDef index : indexes) {
      if (index.getType() == type) {
        nr++;
      }
    }
    return nr;
  }
}
