package org.sirix.cache;

import org.sirix.page.PageKind;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class LogKeyBinding extends TupleBinding<LogKey> {
	@Override
	public LogKey entryToObject(TupleInput in) {
		final ByteArrayDataInput data = ByteStreams.newDataInput(in
				.getBufferBytes());
		return new LogKey(PageKind.getKind(data.readByte()), data.readInt(), data.readInt());
	}

	@Override
	public void objectToEntry(LogKey key, TupleOutput out) {
		final ByteArrayDataOutput output = ByteStreams.newDataOutput();
		output.writeByte(key.getPageKind().getID());
		output.writeInt(key.getLevel());
		output.writeInt(key.getOffset());
		out.write(output.toByteArray());
	}
}
