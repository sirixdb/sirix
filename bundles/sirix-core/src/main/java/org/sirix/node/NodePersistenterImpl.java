package org.sirix.node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

import javax.annotation.Nonnegative;

import org.sirix.api.PageReadTrx;
import org.sirix.node.interfaces.NodePersistenter;
import org.sirix.node.interfaces.Record;

/**
 * Serialize and deserialize nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class NodePersistenterImpl implements NodePersistenter {
	@Override
	public Record deserialize(final DataInput source,
			final @Nonnegative long recordID, final Optional<SirixDeweyID> deweyID,
			final PageReadTrx pageReadTrx) throws IOException {
		final byte id = source.readByte();
		final Kind enumKind = Kind.getKind(id);
		return enumKind.deserialize(source, recordID, deweyID, pageReadTrx);
	}

	@Override
	public void serialize(final DataOutput sink, final Record record,
			final PageReadTrx pageReadTrx) throws IOException {
		final Kind nodeKind = (Kind) record.getKind();
		final byte id = nodeKind.getId();
		sink.writeByte(id);
		nodeKind.serialize(sink, record, pageReadTrx);
	}

	@Override
	public Optional<SirixDeweyID> deserializeDeweyID(final DataInput source,
			Optional<SirixDeweyID> previousDeweyID, final PageReadTrx pageReadTrx)
			throws IOException {
		final byte id = source.readByte();
		final Kind enumKind = Kind.getKind(id);
		return enumKind.deserializeDeweyID(source, previousDeweyID, pageReadTrx);
	}

	@Override
	public void serializeDeweyID(final DataOutput sink, final Kind nodeKind,
			final SirixDeweyID deweyID, final Optional<SirixDeweyID> previousDeweyID,
			final PageReadTrx pageReadTrx) throws IOException {
		final byte id = nodeKind.getId();
		sink.writeByte(id);
		nodeKind.serializeDeweyID(sink, nodeKind, deweyID, previousDeweyID,
				pageReadTrx);
	}
}
