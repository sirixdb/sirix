package org.sirix.xquery.function.jn.diff;

import junit.framework.TestCase;
import net.bytebuddy.implementation.bind.MethodDelegationBinder;
import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
import org.sirix.service.json.serialize.JsonSerializer;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public final class DiffTest extends TestCase {

    private static final String FIRST_DIFF = "{\"database\":\"json-path1\",\"resource\":\"shredded\",\"old-revision\":1,\"new-revision\":3,\"diffs\":[{\"insert\":{\"oldNodeKey\":2,\"newNodeKey\":26,\"insertPositionNodeKey\":1,\"insertPosition\":\"asFirstChild\",\"type\":\"jsonFragment\",\"data\":\"{\\\"tadaaa\\\":\\\"todooo\\\"}\"}},{\"insert\":{\"oldNodeKey\":5,\"newNodeKey\":31,\"insertPositionNodeKey\":4,\"insertPosition\":\"asRightSibling\",\"type\":\"boolean\",\"data\":false}},{\"replace\":{\"oldNodeKey\":5,\"newNodeKey\":28,\"type\":\"jsonFragment\",\"data\":\"{\\\"test\\\":1}\"}},{\"update\":{\"nodeKey\":6,\"type\":\"number\",\"value\":1.2}},{\"delete\":9},{\"delete\":13},{\"update\":{\"nodeKey\":15,\"name\":\"tadaa\"}},{\"update\":{\"nodeKey\":22,\"type\":\"boolean\",\"value\":true}}]}";
    @Override
    protected void setUp() throws Exception {
        JsonTestHelper.deleteEverything();
    }

    @Override
    protected void tearDown() {
        JsonTestHelper.closeEverything();
    }

    @Test
    public void test_whenMultipleRevisionsExist_thenDiff() throws IOException {
        JsonTestHelper.createTestDocument();

        final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
            final var writer = new StringWriter();
            final var wtx = manager.beginNodeTrx()) {
            wtx.moveToDocumentRoot().trx().moveToFirstChild();
            wtx.insertObjectRecordAsFirstChild("tadaaa", new StringValue("todooo"));
            wtx.moveTo(5);
            wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"test\":1}"));
            wtx.moveTo(5);
            wtx.remove();
            wtx.moveTo(4);
            wtx.insertBooleanValueAsRightSibling(true);
            wtx.setBooleanValue(false);
            wtx.moveTo(6);
            wtx.setNumberValue(1.2);
            wtx.moveTo(9);
            wtx.remove();
            wtx.moveTo(13);
            wtx.remove();
            wtx.moveTo(15);
            wtx.setObjectKeyName("tadaa");
            wtx.moveTo(22);
            wtx.setBooleanValue(true);
            wtx.commit();
        }

        // Initialize query context and store.
        try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
            final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
            final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

            // Use XQuery to store a JSON string into the store.
            final String databaseName = PATHS.PATH1.getFile().getName(PATHS.PATH1.getFile().getNameCount() - 1).toString();
            final String resourceName = JsonTestHelper.RESOURCE;

            final var queryBuilder = new StringBuilder();
            queryBuilder.append("jn:diff('");
            queryBuilder.append(databaseName);
            queryBuilder.append("','");
            queryBuilder.append(resourceName);
            queryBuilder.append("',1,3)");

            try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                new XQuery(chain, queryBuilder.toString()).serialize(ctx, new PrintStream(out));
                final String content = new String(out.toByteArray(), StandardCharsets.UTF_8);
                System.out.println(content);
                assertEquals(FIRST_DIFF, content);
            }
        }
    }
}

