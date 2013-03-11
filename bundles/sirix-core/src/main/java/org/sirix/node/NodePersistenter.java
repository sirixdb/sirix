package org.sirix.node;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersistenter;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * Persist nodes.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class NodePersistenter implements RecordPersistenter {
	@Override
	public Record deserialize(final @Nonnull ByteArrayDataInput source,
			final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
		final byte id = source.readByte();
		final Kind enumKind = Kind.getKind(id);
		return enumKind.deserialize(source, recordID, pageReadTrx);
	}

	@Override
	public void serialize(final @Nonnull ByteArrayDataOutput sink,
			final @Nonnull Record record, final @Nullable Record nextRecord,
			final @Nonnull PageReadTrx pageReadTrx) {
		final Kind nodeKind = (Kind) record.getKind();
		final byte id = nodeKind.getId();
		sink.writeByte(id);
		nodeKind.serialize(sink, record, nextRecord, pageReadTrx);
	}
}
