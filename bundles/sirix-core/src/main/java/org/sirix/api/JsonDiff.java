package org.sirix.api;

import org.sirix.api.json.JsonResourceManager;

public interface JsonDiff {
  String generateDiff(JsonResourceManager resourceManager, int oldRevisionNumber, int newRevisionNumber);

  String generateDiff(JsonResourceManager resourceManager, int oldRevisionNumber, int newRevisionNumber, long startNodeKey, long maxDepth);
}
