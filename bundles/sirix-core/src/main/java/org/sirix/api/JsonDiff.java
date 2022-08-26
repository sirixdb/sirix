package org.sirix.api;

import org.sirix.api.json.JsonResourceSession;

public interface JsonDiff {
  String generateDiff(JsonResourceSession resourceManager, int oldRevisionNumber, int newRevisionNumber);

  String generateDiff(JsonResourceSession resourceManager, int oldRevisionNumber, int newRevisionNumber, long startNodeKey, long maxDepth);
}
