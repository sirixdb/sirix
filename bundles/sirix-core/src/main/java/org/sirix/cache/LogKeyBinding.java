package org.sirix.cache;

import javax.annotation.Nonnull;

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
public final class LogKeyBinding extends TupleBinding<LogKey> {
	@Override
	public LogKey entryToObject(final @Nonnull TupleInput in) {
		return new LogKey(PageKind.getKind(in.readByte()), in.readInt(), in.readInt());
	}

	@Override
	public void objectToEntry(final @Nonnull LogKey key, final @Nonnull TupleOutput out) {
		out.writeByte(key.getPageKind().getID());
		out.writeInt(key.getLevel());
		out.writeInt(key.getOffset());
	}
}
