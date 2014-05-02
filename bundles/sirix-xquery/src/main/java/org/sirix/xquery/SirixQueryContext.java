package org.sirix.xquery;

import static com.google.common.base.Preconditions.checkNotNull;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
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
			((DBStore) getStore()).commitAll();
		}
	}
}
