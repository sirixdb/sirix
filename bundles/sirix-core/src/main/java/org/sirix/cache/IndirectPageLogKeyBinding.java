package org.sirix.cache;

import org.sirix.page.PageKind;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Log key binding.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 *
 */
public final class IndirectPageLogKeyBinding extends TupleBinding<IndirectPageLogKey> {
	@Override
	public IndirectPageLogKey entryToObject(final TupleInput in) {
		return new IndirectPageLogKey(PageKind.getKind(in.readByte()), in.readInt(), in.readInt(),
				in.readInt(), in.readLong());
	}

	@Override
	public void objectToEntry(final IndirectPageLogKey key, final TupleOutput out) {
		out.writeByte(key.getPageKind().getID());
		out.writeInt(key.getIndex());
		out.writeInt(key.getLevel());
		out.writeInt(key.getOffset());
		out.writeLong(key.getPageKey());
	}
}
