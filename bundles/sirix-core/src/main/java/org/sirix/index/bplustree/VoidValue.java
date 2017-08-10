package org.sirix.index.bplustree;

import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersistenter;

/**
 * Represents a void value, that is no value at all (for inner node pages in the BPlusTree).
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class VoidValue implements Record {
	@Override
	public long getNodeKey() {
		return 0;
	}

	@Override
	public RecordPersistenter getKind() {
		return null;
	}

	@Override
	public long getRevision() {
		return 0;
	}
}
