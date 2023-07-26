package org.sirix.query.compiler.optimizer.walker.json;

import org.brackit.xquery.compiler.AST;

import java.util.Deque;

public record PathData(Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes,
                       Deque<QueryPathSegment> predicatePathSegmentNamesToArrayIndexes, AST node,
                       AST predicateLeafNode) {
}
