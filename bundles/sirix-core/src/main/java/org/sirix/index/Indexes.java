package org.sirix.index;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.jdm.Stream;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.jdm.node.Node;
import org.brackit.xquery.node.parser.FragmentHelper;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author Karsten Schmidt
 * @author Sebastian Baechle
 *
 */
public final class Indexes implements Materializable {
  public static final QNm INDEXES_TAG = new QNm("indexes");

  private final Set<IndexDef> indexes;

  public Indexes() {
    indexes = new HashSet<>();
  }

  public synchronized Set<IndexDef> getIndexDefs() {
    return new HashSet<>(indexes);
  }

  public synchronized IndexDef getIndexDef(final @NonNegative int indexNo, final IndexType type) {
    checkArgument(indexNo >= 0, "indexNo must be >= 0!");
    for (final IndexDef sid : indexes) {
      if (sid.getID() == indexNo && sid.getType() == type) {
        return sid;
      }
    }
    return null;
  }

  @Override
  public synchronized void init(final Node<?> root) throws DocumentException {
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

  @Override
  public synchronized Node<?> materialize() throws DocumentException {
    FragmentHelper helper = new FragmentHelper();
    helper.openElement(INDEXES_TAG);

    for (IndexDef idxDef : indexes) {
      helper.insert(idxDef.materialize());
    }

    helper.closeElement();
    return helper.getRoot();
  }

  public synchronized void add(IndexDef indexDefinition) {
    indexes.add(indexDefinition);
  }

  public synchronized void removeIndex(final @NonNegative int indexID) {
    checkArgument(indexID >= 0, "indexID must be >= 0!");
    for (final IndexDef indexDef : indexes) {
      if (indexDef.getID() == indexID) {
        indexes.remove(indexDef);
        return;
      }
    }
  }

  public Optional<IndexDef> findPathIndex(final Path<QNm> path) throws DocumentException {
    requireNonNull(path);
    try {
      for (final IndexDef index : indexes) {
        if (index.isPathIndex()) {
          if (index.getPaths().isEmpty()) {
            return Optional.of(index);
          }

          for (final Path<QNm> indexedPath : index.getPaths()) {
            if (indexedPath.matches(path)) {
              return Optional.of(index);
            }
          }
        }
      }
      return Optional.empty();
    } catch (PathException e) {
      throw new DocumentException(e);
    }
  }

  public Optional<IndexDef> findCASIndex(final Path<QNm> path, final Type type)
      throws DocumentException {
    requireNonNull(path);
    try {
      for (final IndexDef index : indexes) {
        if (index.isCasIndex() && index.getContentType().equals(type)) {
          if (index.getPaths().isEmpty()) {
            return Optional.of(index);
          }

          for (final Path<QNm> indexedPath : index.getPaths()) {
            if (indexedPath.matches(path)) {
              return Optional.of(index);
            }
          }
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
