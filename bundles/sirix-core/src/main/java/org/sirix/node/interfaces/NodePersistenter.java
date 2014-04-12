package org.sirix.node.interfaces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.sirix.api.PageReadTrx;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;

import com.google.common.base.Optional;

public interface NodePersistenter extends RecordPersistenter {
	Optional<SirixDeweyID> deserializeDeweyID(DataInput source, Optional<SirixDeweyID> previousDeweyID,
			PageReadTrx pageReadTrx) throws IOException;

	void serializeDeweyID(DataOutput sink, Kind nodeKind,
			SirixDeweyID deweyID, Optional<SirixDeweyID> nextDeweyID,
			PageReadTrx pageReadTrx) throws IOException;
}
