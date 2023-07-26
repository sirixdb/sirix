package io.sirix.cache;

import io.sirix.index.IndexType;

public record RBIndexKey(long nodeKey, int revisionNumber, IndexType indexType, int indexNumber) {
}
