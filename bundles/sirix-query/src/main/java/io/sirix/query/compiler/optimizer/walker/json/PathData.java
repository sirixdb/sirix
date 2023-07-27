package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;

import java.util.Deque;

public record PathData(Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes,
                       Deque<QueryPathSegment> predicatePathSegmentNamesToArrayIndexes, AST node,
                       AST predicateLeafNode) {
}
