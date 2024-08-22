package io.sirix.query.json;

import io.brackit.query.util.serialize.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BasicJsonDBStoreTest {

	private BasicJsonDBStore.Builder builder;

	@BeforeEach
	void setUp() {
		builder = BasicJsonDBStore.newBuilder();
	}

	@SuppressWarnings("DataFlowIssue")
	@Test
	@DisplayName("Should create a new collection with provided JSON")
	void shouldCreateNewCollectionWithProvidedJson() {
		String collName = "testCollection";
		String optResName = "testResource";
		String json = "{\"key\":\"value\"}";
		final var store = builder.build();
		store.create(collName, optResName, json);
		JsonDBCollection collection = store.lookup(collName);
		JsonDBItem testResource = collection.getDocument("testResource");
		final var stringWriter = new StringWriter();
		final var writer = new PrintWriter(stringWriter);
		new StringSerializer(writer).serialize(testResource);
		assertEquals(json, stringWriter.toString());
	}

	@Test
	@DisplayName("Should set correct number of nodes before auto commit")
	void shouldSetCorrectNumberOfNodesBeforeAutoCommit() {
		int expectedNodes = 500;
		builder.numberOfNodesBeforeAutoCommit(expectedNodes);
		var store = builder.build();
		assertEquals(expectedNodes, store.options().numberOfNodesBeforeAutoCommit());
	}
}
