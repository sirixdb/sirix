package io.sirix.query.json;

import io.sirix.query.stream.json.TemporalSirixJsonArrayStream;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Array;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.temporal.AllTimeAxis;
import io.sirix.axis.temporal.FutureAxis;
import io.sirix.axis.temporal.PastAxis;
import io.sirix.query.StructuredDBItem;

import static java.util.Objects.requireNonNull;

public final class JsonDBArray extends AbstractJsonDBArray<JsonDBArray>
		implements
			TemporalJsonDBItem<JsonDBArray>,
			Array,
			JsonDBItem,
			StructuredDBItem<JsonNodeReadOnlyTrx> {

	/** Sirix read-only transaction. */
	private final JsonNodeReadOnlyTrx rtx;

	/** Collection this node is part of. */
	private final JsonDBCollection collection;

	/**
	 * Constructor.
	 *
	 * @param rtx
	 *            {@link JsonNodeReadOnlyTrx} for providing reading access to the
	 *            underlying node
	 * @param collection
	 *            {@link JsonDBCollection} reference
	 */
	public JsonDBArray(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
		super(rtx, collection, new JsonItemFactory());
		this.collection = requireNonNull(collection);
		this.rtx = requireNonNull(rtx);

		if (this.rtx.isDocumentRoot()) {
			this.rtx.moveToFirstChild();
		}

		assert this.rtx.isArray();
	}

	@Override
	public Stream<JsonDBArray> getEarlier(final boolean includeSelf) {
		moveRtx();
		final IncludeSelf include = includeSelf ? IncludeSelf.YES : IncludeSelf.NO;
		return new TemporalSirixJsonArrayStream(new PastAxis<>(rtx.getResourceSession(), rtx, include), collection);
	}

	@Override
	public Stream<JsonDBArray> getFuture(final boolean includeSelf) {
		moveRtx();
		final IncludeSelf include = includeSelf ? IncludeSelf.YES : IncludeSelf.NO;
		return new TemporalSirixJsonArrayStream(new FutureAxis<>(rtx.getResourceSession(), rtx, include), collection);
	}

	@Override
	public Stream<JsonDBArray> getAllTimes() {
		moveRtx();
		return new TemporalSirixJsonArrayStream(new AllTimeAxis<>(rtx.getResourceSession(), rtx), collection);
	}

	@Override
	public Array range(IntNumeric from, IntNumeric to) {
		moveRtx();

		return new JsonDBArraySlice(rtx, collection, from.intValue(), to.intValue());
	}

	@Override
	protected JsonDBArray createInstance(JsonNodeReadOnlyTrx rtx, JsonDBCollection collection) {
		return new JsonDBArray(rtx, collection);
	}
}
