# Reversible agent edits

A Sirix resource is one versioned JSON or XML document. Record the database,
resource, starting revision and commit message for each agent edit. Read an
explicit revision, inspect the diff, then restore a retained revision if needed.
Revert restores the **whole resource**, including unrelated edits since that
revision; it appends a new revision and preserves the intervening history.
There is no branch or multi-resource atomic revert in this workflow.

## REST: read, diff, revert, reopen

Start the server using the [REST quickstart](QUICKSTART.md). These examples use
its local HTTP development configuration. With Keycloak, add
`-H "Authorization: Bearer $TOKEN"` to every data request. Revert requires
`modify` or `<database>-modify`; reads and diffs require `view`.

After QUICKSTART steps 1–6, `mydb/products` has two revisions:

```bash
# Capture the head and its commit timestamp before planning an edit:
curl --fail-with-body -s http://localhost:9443/mydb/products/history

# Reproducible as-of read by revision:
curl --fail-with-body -s 'http://localhost:9443/mydb/products?revision=1'

# As-of read by commit timestamp (replace with a timestamp from history):
curl --fail-with-body -sG http://localhost:9443/mydb/products \
  --data-urlencode 'revision-timestamp=2026-06-11T13:15:08.577Z'

# Compare an increasing pair of revisions; response is a JSON object:
curl --fail-with-body -s \
  'http://localhost:9443/mydb/products/diff?first-revision=1&second-revision=2'

# Restore revision 1 only if the head is still 2:
curl --fail-with-body -s -X POST http://localhost:9443/mydb/products/revert \
  -H 'Content-Type: application/json' \
  -d '{"targetRevision":1,"expectedHead":2,"message":"Undo agent product edit"}'
# {"revision":3,"previousHead":2}

curl --fail-with-body -s 'http://localhost:9443/mydb/products?revision=3'
# Original two products
curl --fail-with-body -s 'http://localhost:9443/mydb/products?revision=2'
# Agent edit (monitor) still visible in history
```

The same revert endpoint works for XML databases and returns JSON metadata.
The [OpenAPI specification](../bundles/sirix-rest-api/src/main/resources/openapi.yaml),
served at `GET /openapi.yaml`, owns the request schema and response codes.
Agents should supply `expectedHead` and save `revision` and `previousHead` from
the response. If the head changed, re-read history and diff before replanning.
If the target was pruned, select an available target explicitly.

A timeout does not prove the commit failed. Inspect history before retrying;
revert is not idempotent. Retry with the original `expectedHead` to avoid
silently appending a second restoration after a successful first request.

For a cold reopen, close the server normally and start it again with the same
configuration and persistent data directory. Repeat the explicit revision 2 and
3 reads above. With Docker, retain the data volume. Restarting with a different
home/data directory opens different storage.

## JVM: explicit transaction ownership

Use JDK 25 with the preview/native-access flags described in the
[REST quickstart](QUICKSTART.md), and the `sirix-core` dependency for the following
Java program. It creates a fresh local JSON database, writes two revisions,
resolves an as-of timestamp, computes a diff and reverts under the writer lock.
The example owns and closes every handle. Use a fresh path when running again.

```java
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Instant;

public final class AgentRoundTrip {
  public static void main(String[] args) {
    final Path path = Path.of("agent-demo").toAbsolutePath();
    if (!Databases.createJsonDatabase(new DatabaseConfiguration(path))) {
      throw new IllegalStateException("Use a fresh database path");
    }
    try (final var db = Databases.openJsonDatabase(path)) {
      db.createResource(ResourceConfiguration.newBuilder("state").build());
      try (final var session = db.beginResourceSession("state")) {
        try (final var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"draft\"]"),
              JsonNodeTrx.Commit.NO);
          wtx.commit("initial"); // revision 1
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // array
          wtx.moveToFirstChild(); // string
          wtx.setStringValue("published");
          wtx.commit("agent edit"); // revision 2
        }
        final Instant asOf;
        try (final var rtx = session.beginNodeReadOnlyTrx(1)) {
          asOf = rtx.getRevisionTimestamp();
        }
        final int ceiling = session.getMostRecentRevisionNumber();
        final int selected = session.getRevisionNumber(asOf, ceiling);
        try (final var rtx = session.beginNodeReadOnlyTrx(selected)) {
          System.out.println("As of: revision " + rtx.getRevisionNumber());
        }
        System.out.println(new BasicJsonDiff(db.getName()).generateDiff(session, 1, 2));
        try (final var wtx = session.beginNodeTrx()) {
          final int previousHead = wtx.getRevisionNumber() - 1;
          if (previousHead != 2) {
            throw new IllegalStateException("Head changed: " + previousHead);
          }
          wtx.revertTo(1);
          wtx.commit("Undo agent edit"); // revision 3, whole resource
        }
      }
    }
    // Cold reopen: no database, session or transaction handle survives above.
    try (final var db = Databases.openJsonDatabase(path);
         final var session = db.beginResourceSession("state")) {
      for (final int revision : new int[] {1, 2, 3}) {
        final var json = new StringWriter();
        JsonSerializer.newBuilder(session, json, revision).build().call();
        System.out.println(revision + ": " + json);
      }
      // 1: ["draft"], 2: ["published"], 3: ["draft"]
    }
  }
}
```

XML uses `Databases.openXmlDatabase`, an XML resource session and `XmlSerializer`;
`beginNodeTrx()` → check head → `revertTo(N)` → `commit(message)` has the same
whole-resource meaning. A writer must be exclusive and closed promptly. Do not
check the head before acquiring it: another writer could commit between the
check and restoration. No core or Kotlin helper is required for this sequence.

## JSONiq/query

Against the same local `agent-demo/state` database, use a `BasicJsonDBStore`
whose `location` is the parent directory containing `agent-demo`, with
`SirixCompileChain.createWithJsonStore(store)` and
`SirixQueryContext.createWithJsonStore(store)`. REST `POST /` also accepts these
expressions as `{"query":"..."}` using server-side database names/paths.
Run each expression separately:

```xquery
(: Read an explicit revision :)
jn:doc('agent-demo', 'state', 1)

(: As of a commit timestamp copied from history :)
jn:open('agent-demo', 'state', xs:dateTime('2026-06-11T13:15:08.577Z'))

(: Diff returns object(), so its fields are directly queryable :)
let $diff as object() := jn:diff('agent-demo', 'state', 1, 2)
return $diff.diffs

(: Appends a whole-resource restoration, returning the new revision number :)
sdb:commit(jn:doc('agent-demo', 'state', 1), 'Undo agent edit')
```

`jn:diff` accepts four arguments, optionally followed by a start node key and
maximum depth. The revisions must satisfy `1 <= old < new`. Diff subtree
filtering does not change the scope of revert. The query commit expression has
**no atomic expected-head parameter**; use the REST revert endpoint or a JVM
writer for the guarded operation. A query-level read/check followed by commit
is not an equivalent concurrency guard.

To verify a query cold reopen, close the query context, compile chain and store,
construct fresh ones with the same store location, and query revisions 1, 2 and
the revision returned by `sdb:commit`. With the server, restart as described
above and run the same explicit-revision queries. The CLI's local query command
can evaluate these expressions; consult `sirix-cli --help` and command help for
its options. CLI `restore` means database-backup restoration.

## Timestamp selection and retained history

Timestamps refer to **commit/system time** for a resource. They are not a
predicate over application fields, and independent resources have independent
revision sequences. Capture explicit revision IDs for reproducible work.

| Case | Behavior |
|---|---|
| Between commits | Floor: selects the preceding revision. |
| After the latest commit | Selects the latest revision (or the supplied JVM ceiling). |
| Exact timestamp, including ties | Lookup compares at millisecond precision. The one-argument JVM lookup, REST and `jn:open` do not promise which equal-timestamp revision wins. Pin a revision ID. |
| Deterministic JVM selection | `getRevisionNumber(instant, ceiling)` chooses the highest revision at or below the ceiling with timestamp <= instant; capture the ceiling once. |
| Before all revisions | Core selects bootstrap revision 0. REST GET/HEAD clamp that result to revision 1; `jn:open` can return the empty sequence before document creation. Do not interpret REST's first document as proof it existed at the requested time. |
| Pruned selection | Metadata lookup can still return the original revision number/timestamp. Opening that revision fails; it does not silently substitute a retained neighbor. |

Prefer ISO 8601 timestamps with `Z` or an explicit offset. REST also accepts
zone-less date-times as UTC; encode `+` in offsets using `--data-urlencode`.
The JVM lookup accepts `Instant`; `jn:open` accepts `xs:dateTime` (use an explicit
timezone). Revision IDs avoid timestamp ties and precision loss.

Retention can remove readable historical roots while leaving timestamp
metadata. As-of reads, diffs and reverts require the selected revision roots to
remain readable. Core signals `SirixRevisionPrunedException`; the new REST revert
maps a pruned target to 410. Existing REST read/query failure mappings are
separate and do not guarantee 410. Full history including authors/messages also
requires readable roots; core `getHistoryTimestamps()` remains suitable for
metadata across pruned history. There is no retention configuration or pruning
command introduced by this guide. Keep required history/verified backups before
applying a separate retention policy.
