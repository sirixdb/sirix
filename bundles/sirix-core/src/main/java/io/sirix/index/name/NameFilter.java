package io.sirix.index.name;

import io.brackit.query.atomic.QNm;

import java.util.Collections;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class NameFilter {

  private final Set<QNm> includes;

  private final Set<QNm> excludes;

  public NameFilter(final Set<QNm> included, final Set<QNm> excluded) {
    includes = Collections.unmodifiableSet(requireNonNull(included));
    excludes = Collections.unmodifiableSet(requireNonNull(excluded));
  }

  public Set<QNm> getIncludes() {
    return includes;
  }

  public Set<QNm> getExcludes() {
    return excludes;
  }
}
