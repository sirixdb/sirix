package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.atomic.Str;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class XmlDescendantQueryTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"false, true", "true, true", "false, false", "true, false"})
  void namedDescendantPlansStartAtTheirNonRootContext(final boolean storeDeweyIds, final boolean pathSummary) {
    final String[] documents = {"<r><a><hit id='h1'/></a></r>",
        "<r><a><branch><hit id='h1'/></branch></a><noise/></r>",
        "<r><a><left><hit id='h1'/></left><right><hit id='h2'/></right></a></r>",
        "<r><a><hit id='h1'/><branch><hit id='h2'/></branch></a></r>",
        "<r><a><hit id='self'><branch><hit id='child'/></branch></hit></a></r>",
        "<r><a><hit id='self'/></a></r>"};
    final String[] descendants = {"h1", "h1", "h1,h2", "h1,h2", "child", ""};
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder()
                                                    .location(directory)
                                                    .storageType(StorageType.FILE_CHANNEL)
                                                    .storeDeweyIds(storeDeweyIds)
                                                    .buildPathSummary(pathSummary)
                                                    .build();
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      for (int index = 0; index < documents.length; index++) {
        final String resource = "tree" + index;
        assertNotNull(collection.add(resource, new DocumentParser(documents[index])));
        final String start = "xml:doc('collection','" + resource + "')/r/a" + (index >= 4 ? "/hit" : "");
        for (final String axis : new String[] {"descendant", "descendant-or-self"}) {
          final String query = "string-join(for $hit in " + start + "/" + axis
              + "::hit return string($hit/@id), ',')";
          final String expected = axis.equals("descendant-or-self") && index >= 4
              ? (index == 4 ? "self,child" : "self")
              : descendants[index];
          assertEquals(expected, ((Str) new Query(chain, query).evaluate(context)).stringValue(), query);
        }
      }
    }
  }
}
