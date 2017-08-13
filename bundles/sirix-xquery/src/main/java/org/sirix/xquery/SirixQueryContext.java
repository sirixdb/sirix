package org.sirix.xquery;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.update.op.UpdateOp;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.xquery.node.DBNode;
import org.sirix.xquery.node.DBStore;

public final class SirixQueryContext extends QueryContext {

	public enum CommitStrategy {
		AUTO,

		EXPLICIT
	}

	private CommitStrategy mCommitStrategy;

	public SirixQueryContext(final DBStore store) {
		this(store, CommitStrategy.AUTO);
	}

	public SirixQueryContext(final DBStore store, final CommitStrategy commitStrategy) {
		super(store);
		mCommitStrategy = checkNotNull(commitStrategy);
	}

	@Override
	public void applyUpdates() throws QueryException {
		super.applyUpdates();

		if (mCommitStrategy == CommitStrategy.AUTO) {
			final List<UpdateOp> updateList =
					getUpdateList() != null ? getUpdateList().list() : Collections.emptyList();

			if (!updateList.isEmpty()) {
				final Function<Sequence, XdmNodeReadTrx> mapDBNodeToWtx = sequence -> {
					if (sequence instanceof DBNode) {
						final XdmNodeReadTrx trx = ((DBNode) sequence).getTrx();
						return trx;
					}

					// Can not occur (must be a write transaction).
					throw new IllegalStateException();
				};

				final Set<Long> trxIDs = new HashSet<>();

				updateList.stream().map(UpdateOp::getTarget).map(mapDBNodeToWtx)
						.filter(trx -> trxIDs.add(trx.getId()))
						.forEach(trx -> ((XdmNodeWriteTrx) trx).commit());
			}
		}
	}
}
