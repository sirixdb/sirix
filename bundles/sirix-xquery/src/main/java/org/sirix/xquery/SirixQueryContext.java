package org.sirix.xquery;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.update.op.UpdateOp;
import org.brackit.xquery.xdm.Sequence;
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
				commit(updateList.get(0).getTarget());
			}
		}
	}

	private void commit(Sequence item) {
		if (item instanceof DBNode) {
			final Optional<XdmNodeWriteTrx> trx =
					((DBNode) item).getTrx().getResourceManager().getNodeWriteTrx();
			trx.ifPresent(XdmNodeWriteTrx::commit);
		}
	}
}
