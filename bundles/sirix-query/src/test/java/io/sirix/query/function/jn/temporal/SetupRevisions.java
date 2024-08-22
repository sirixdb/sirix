package io.sirix.query.function.jn.temporal;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.JsonDBObject;
import io.brackit.query.Query;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.JsonDocumentCreator;

import java.io.IOException;

public final class SetupRevisions {
	private SetupRevisions() {
	}

	public static void setupRevisions(final SirixQueryContext ctx, final SirixCompileChain chain) throws IOException {
		final var storeQuery = "jn:store('json-path1','mydoc.jn','" + JsonDocumentCreator.JSON + "')";
		new Query(chain, storeQuery).evaluate(ctx);

		final var openDocQuery = "jn:doc('json-path1','mydoc.jn')";
		final var object = (JsonDBObject) new Query(chain, openDocQuery).evaluate(ctx);

		try (final var wtx = object.getTrx().getResourceSession().beginNodeTrx()) {
			wtx.moveTo(3);

			try (final var reader = JsonShredder.createStringReader("{\"foo\":\"bar\"}")) {
				wtx.insertSubtreeAsFirstChild(reader);
				wtx.commit();

				wtx.moveTo(11);
				wtx.remove();
				wtx.commit();

				wtx.moveTo(25);
				wtx.insertArrayAsRightSibling();
				wtx.commit();
			}
		}
	}
}
