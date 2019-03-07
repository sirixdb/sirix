package org.sirix.axis;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.utils.Pair;
import com.google.common.collect.AbstractIterator;

/**
 * TemporalAxis abstract class.
 *
 * @author Johannes Lichtenberger
 *
 */
public abstract class AbstractTemporalAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractIterator<Pair<Integer, Long>> {

  public abstract ResourceManager<R, W> getResourceManager();
}
