package org.sirix.cache;

import org.sirix.index.IndexType;

public record RBIndexKey(long nodeKey, int revisionNumber, IndexType indexType, int indexNumber) {
}
