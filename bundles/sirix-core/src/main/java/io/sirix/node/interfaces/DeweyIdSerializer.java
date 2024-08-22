package io.sirix.node.interfaces;

import io.sirix.access.ResourceConfiguration;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

public interface DeweyIdSerializer extends RecordSerializer {
	IntegratedIntCompressor INTEGRATED_INT_COMPRESSOR = new IntegratedIntCompressor();

	byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig);

	void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID, ResourceConfiguration resourceConfig);
}
