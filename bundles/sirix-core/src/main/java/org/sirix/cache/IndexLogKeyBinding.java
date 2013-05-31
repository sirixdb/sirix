package org.sirix.cache;

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
	public IndexLogKey entryToObject(final TupleInput in) {
		return new IndexLogKey(in.readLong(), in.readInt());
	}

	@Override
	public void objectToEntry(final IndexLogKey key, final TupleOutput out) {
		out.writeLong(key.getRecordPageKey());
		out.writeInt(key.getIndex());
	}
}
