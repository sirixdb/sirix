package org.sirix.node;

import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersistenter;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * Persistenting nodes.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class NodePersistenter implements RecordPersistenter {
	@Override
	public Record deserialize(@Nonnull ByteArrayDataInput source,
			@Nonnull PageReadTrx pageReadTrx) {
		final byte id = source.readByte();
		final Kind enumKind = Kind.getKind(id);
		return enumKind.deserialize(source, pageReadTrx);
	}

	@Override
	public void serialize(@Nonnull ByteArrayDataOutput sink,
			@Nonnull Record toSerialize, @Nonnull PageReadTrx pageReadTrx) {
		final Kind nodeKind = (Kind) toSerialize.getKind();
		final byte id = nodeKind.getId();
		sink.writeByte(id);
		nodeKind.serialize(sink, toSerialize, pageReadTrx);
	}
}
