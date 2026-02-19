package io.sirix.api;

import io.sirix.api.json.JsonResourceSession;

public interface JsonDiff {
  String generateDiff(JsonResourceSession resourceSession, int oldRevisionNumber, int newRevisionNumber);

  String generateDiff(JsonResourceSession resourceSession, int oldRevisionNumber, int newRevisionNumber,
      long startNodeKey, long maxDepth);
}
