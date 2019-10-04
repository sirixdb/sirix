package org.sirix.axis;

import com.google.common.collect.AbstractIterator;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;

/**
 * TemporalAxis abstract class.
 *
 * @author Johannes Lichtenberger
 *
 */
public abstract class AbstractTemporalAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractIterator<R> {

  public abstract ResourceManager<R, W> getResourceManager();
}
