package org.sirix.node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersistenter;

/**
 * Persist nodes.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class NodePersistenter implements RecordPersistenter {
	@Override
	public Record deserialize(final DataInputStream source,
			final @Nonnegative long recordID, final PageReadTrx pageReadTrx) throws IOException {
		final byte id = source.readByte();
		final Kind enumKind = Kind.getKind(id);
		return enumKind.deserialize(source, recordID, pageReadTrx);
	}

	@Override
	public void serialize(final DataOutputStream sink,
			final Record record, final @Nullable Record nextRecord,
			final PageReadTrx pageReadTrx) throws IOException {
		final Kind nodeKind = (Kind) record.getKind();
		final byte id = nodeKind.getId();
		sink.writeByte(id);
		nodeKind.serialize(sink, record, nextRecord, pageReadTrx);
	}
}
