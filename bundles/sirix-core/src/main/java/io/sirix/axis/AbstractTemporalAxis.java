package io.sirix.axis;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import com.google.common.collect.AbstractIterator;

/**
 * TemporalAxis abstract class.
 *
 * @author Johannes Lichtenberger
 *
 */
public abstract class AbstractTemporalAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
		extends
			AbstractIterator<R> {

	public abstract ResourceSession<R, W> getResourceManager();
}
