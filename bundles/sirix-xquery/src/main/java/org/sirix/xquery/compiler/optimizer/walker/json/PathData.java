package org.sirix.xquery.compiler.optimizer.walker.json;

import org.brackit.xquery.compiler.AST;

import java.util.Deque;
import java.util.Map;

public record PathData(Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes,
                       Deque<QueryPathSegment> predicatePathSegmentNamesToArrayIndexes, AST node,
                       AST predicateLeafNode) {
}
