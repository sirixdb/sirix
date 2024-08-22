package io.sirix.api;

import io.sirix.api.json.JsonResourceSession;

public interface JsonDiff {
	String generateDiff(JsonResourceSession resourceManager, int oldRevisionNumber, int newRevisionNumber);

	String generateDiff(JsonResourceSession resourceManager, int oldRevisionNumber, int newRevisionNumber,
			long startNodeKey, long maxDepth);
}
