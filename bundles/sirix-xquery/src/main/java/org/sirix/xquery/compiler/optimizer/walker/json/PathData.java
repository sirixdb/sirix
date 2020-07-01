package org.sirix.xquery.compiler.optimizer.walker.json;

import org.brackit.xquery.compiler.AST;

import java.util.Deque;
import java.util.Map;

public record PathData(Deque<String> pathSegmentNames, Map<String, Deque<Integer>>arrayIndexes, Deque<String> predicateNames, AST node) {
}
