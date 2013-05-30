package org.sirix.cache;

import javax.annotation.Nonnull;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Index log key binding.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 *
 */
public final class IndexLogKeyBinding extends TupleBinding<IndexLogKey> {
	@Override
	public IndexLogKey entryToObject(final @Nonnull TupleInput in) {
		return new IndexLogKey(in.readLong(), in.readInt());
	}

	@Override
	public void objectToEntry(final @Nonnull IndexLogKey key, final @Nonnull TupleOutput out) {
		out.writeLong(key.getRecordPageKey());
		out.writeInt(key.getIndex());
	}
}
