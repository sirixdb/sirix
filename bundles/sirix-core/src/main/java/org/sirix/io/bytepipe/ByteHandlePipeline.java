/**
 * 
 */
package org.sirix.io.bytepipe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixIOException;

/**
 * Pipeline to handle Bytes before stored in the backends.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class ByteHandlePipeline implements ByteHandler {

	/** Pipeline hold over here. */
	private final List<ByteHandler> mParts;

	/**
	 * Copy constructor.
	 * 
	 * @param pipeline
	 *          pipeline to copy
	 */
	public ByteHandlePipeline(final @Nonnull ByteHandlePipeline pipeline) {
		mParts = new ArrayList<>(pipeline.mParts.size());
		for (final ByteHandler handler : pipeline.mParts) {
			mParts.add(handler.getInstance());
		}
	}

	/**
	 * 
	 * Constructor.
	 * 
	 * @param parts
	 *          to be stored, Order is important!
	 */
	public ByteHandlePipeline(final @Nonnull ByteHandler... parts) {
		mParts = new ArrayList<>();
		for (final ByteHandler part : parts) {
			mParts.add(part);
		}
	}

	@Override
	public byte[] serialize(final @Nonnull byte[] toSerialize)
			throws SirixIOException {
		byte[] pipeData = toSerialize;
		for (final ByteHandler part : mParts) {
			pipeData = part.serialize(pipeData);
		}
		return pipeData;
	}

	@Override
	public byte[] deserialize(final @Nonnull byte[] toDeserialize)
			throws SirixIOException {
		byte[] pipeData = toDeserialize;
		for (int i = mParts.size() - 1; i >= 0; i--) {
			pipeData = mParts.get(i).deserialize(pipeData);
		}
		return pipeData;
	}

	/**
	 * Get byte handler components.
	 * 
	 * @return all components
	 */
	public List<ByteHandler> getComponents() {
		return Collections.unmodifiableList(mParts);
	}

	@Override
	public ByteHandler getInstance() {
		return new ByteHandlePipeline();
	}

}
