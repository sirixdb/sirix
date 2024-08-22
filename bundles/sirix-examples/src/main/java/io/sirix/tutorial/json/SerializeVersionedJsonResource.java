package io.sirix.tutorial.json;

import io.sirix.tutorial.Constants;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.serialize.JsonSerializer;

import java.io.StringWriter;

public class SerializeVersionedJsonResource {

	public static void main(String[] args) {
		CreateVersionedJsonResource.createJsonDatabaseWithVersionedResource();

		try (final var database = Databases
				.openJsonDatabase(Constants.SIRIX_DATA_LOCATION.resolve("json-database-versioned"));
				final var session = database.beginResourceSession("resource")) {
			serializeRevisionOneAndTwo(session);

			serializeMostRecentRevision(session);

			serializeAllRevisions(session);
		}
	}

	private static void serializeRevisionOneAndTwo(final JsonResourceSession manager) {
		final var writer = new StringWriter();
		final var serializerForRevisionOneAndTwo = new JsonSerializer.Builder(manager, writer, 1, 2).build();
		serializerForRevisionOneAndTwo.call();
		System.out.println("Revision 1 and 2:");
		System.out.println(writer);
	}

	private static void serializeMostRecentRevision(final JsonResourceSession manager) {
		final var writer = new StringWriter();
		final var serializerForMostRecentRevision = new JsonSerializer.Builder(manager, writer).build();
		serializerForMostRecentRevision.call();
		System.out.println("Most recent revision:");
		System.out.println(writer);
	}

	private static void serializeAllRevisions(final JsonResourceSession manager) {
		final var writer = new StringWriter();
		final var serializerForAllRevisions = new JsonSerializer.Builder(manager, writer, -1).build();
		serializerForAllRevisions.call();
		System.out.println("All revisions:");
		System.out.println(writer);
	}
}
