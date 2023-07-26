package org.sirix.query.compiler.optimizer.walker.json;

import java.util.Deque;

public record QueryPathSegment(String pathSegmentName, Deque<Integer> arrayIndexes) {
}
