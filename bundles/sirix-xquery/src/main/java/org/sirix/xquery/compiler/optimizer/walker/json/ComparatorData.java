package org.sirix.xquery.compiler.optimizer.walker.json;

import org.brackit.xquery.atomic.Atomic;

final class ComparatorData {
  private String comparator;
  private Atomic atomic;
  private String upperBoundComparator;
  private Atomic upperBoundAtomic;

  ComparatorData() {
  }

  ComparatorData(String comparator, Atomic atomic, String upperBoundComparator, Atomic upperBoundAtomic) {
    this.comparator = comparator;
    this.atomic = atomic;
    this.upperBoundComparator = upperBoundComparator;
    this.upperBoundAtomic = upperBoundAtomic;
  }

  public Atomic getUpperBoundAtomic() {
    return upperBoundAtomic;
  }

  public void setUpperBoundAtomic(Atomic upperBoundAtomic) {
    this.upperBoundAtomic = upperBoundAtomic;
  }

  public String getUpperBoundComparator() {
    return upperBoundComparator;
  }

  public void setUpperBoundComparator(String upperBoundComparator) {
    this.upperBoundComparator = upperBoundComparator;
  }

  public Atomic getAtomic() {
    return atomic;
  }

  public void setAtomic(Atomic atomic) {
    this.atomic = atomic;
  }

  public String getComparator() {
    return comparator;
  }

  public void setComparator(String comparator) {
    this.comparator = comparator;
  }
}