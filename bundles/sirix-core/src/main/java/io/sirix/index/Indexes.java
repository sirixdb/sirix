package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.node.parser.FragmentHelper;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.google.common.base.Preconditions.checkArgument;
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

  /**
   * Gets an index definition by index number and type. Thread-safe without synchronization due to
   * CopyOnWriteArraySet.
   */
  public IndexDef getIndexDef(final @NonNegative int indexNo, final IndexType type) {
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
        indexes.add(indexDefinition);
      }
    }
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
   * Adds an index definition. Thread-safe: CopyOnWriteArraySet handles concurrent modifications.
   */
  public void add(IndexDef indexDefinition) {
    indexes.add(indexDefinition);
  }

  /**
   * Removes an index definition by ID. Thread-safe: CopyOnWriteArraySet handles concurrent
   * modifications.
   */
  public void removeIndex(final @NonNegative int indexID) {
    checkArgument(indexID >= 0, "indexID must be >= 0!");
    indexes.removeIf(indexDef -> indexDef.getID() == indexID);
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
