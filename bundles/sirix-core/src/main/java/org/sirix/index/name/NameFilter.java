package org.sirix.index.name;

import org.brackit.xquery.atomic.QNm;
import org.sirix.index.Filter;
import org.sirix.index.redblacktree.RBNode;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;

import java.util.Collections;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class NameFilter implements Filter {

  private final Set<QNm> includes;

  private final Set<QNm> excludes;

  public NameFilter(final Set<QNm> included, final Set<QNm> excluded) {
    includes = requireNonNull(included);
    excludes = requireNonNull(excluded);
  }

  public Set<QNm> getIncludes() {
    return Collections.unmodifiableSet(includes);
  }

  public Set<QNm> getExcludes() {
    return Collections.unmodifiableSet(excludes);
  }

  @Override
  public <K extends Comparable<? super K>> boolean filter(final RBNode<K, NodeReferences> node) {
    if (!(node.getKey() instanceof final QNm name))
      throw new IllegalStateException("Key is not of type QNm!");

    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));

    return included && !excluded;
  }
}
