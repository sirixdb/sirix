package org.sirix.xquery.compiler.optimizer.walker.json;

import java.util.Deque;
import java.util.Map;

public record PathSegmentData(Deque<String>pathNames, Map<String, Deque<Integer>>arrayIndexes) {
}
