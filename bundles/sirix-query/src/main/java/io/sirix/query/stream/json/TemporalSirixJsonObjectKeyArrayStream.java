package io.sirix.query.stream.json;

import com.google.common.base.MoreObjects;
import io.brackit.query.jdm.Stream;
import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonObjectKeyDBArray;
import io.sirix.query.node.XmlDBCollection;

import static java.util.Objects.requireNonNull;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalSirixJsonObjectKeyArrayStream implements Stream<JsonObjectKeyDBArray> {

	/** Temporal axis. */
	private final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis;

	/** The {@link JsonDBCollection} reference. */
	private final JsonDBCollection collection;

	/**
	 * Constructor.
	 *
	 * @param axis
	 *            Sirix {@link Axis}
	 * @param collection
	 *            {@link XmlDBCollection} the nodes belong to
	 */
	public TemporalSirixJsonObjectKeyArrayStream(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis,
			final JsonDBCollection collection) {
		this.axis = requireNonNull(axis);
		this.collection = requireNonNull(collection);
	}

	@Override
	public JsonObjectKeyDBArray next() {
		if (axis.hasNext()) {
			final var rtx = axis.next();
			return new JsonObjectKeyDBArray(rtx, collection);
		}
		return null;
	}

	@Override
	public void close() {
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("axis", axis).toString();
	}
}
