package org.sirix.xquery.compiler.optimizer.walker.json;

import java.util.Deque;

public record QueryPathSegment(String pathSegmentName, Deque<Integer> arrayIndexes) {
}
