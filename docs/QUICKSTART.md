# SirixDB Quickstart — REST server in 5 minutes

This walkthrough takes you from nothing to a running SirixDB REST server with a
versioned resource, a JSONiq query, and a time-travel read + diff. Every command
below was executed verbatim against the fat jar built from this tree; the output
snippets are real.

There are three ways to run the server, fastest first:

| Option | Needs | Auth |
|---|---|---|
| [A. Dev mode, no Keycloak](#option-a--dev-mode-no-keycloak-fastest) | Java 25 + the fat jar | none (`auth.mode=none`) |
| [B. Fat jar + Keycloak](#option-b--fat-jar--keycloak) | Java 25, Docker | OAuth2 (Keycloak) |
| [C. Everything in Docker](#option-c--everything-in-docker) | Docker | OAuth2 (Keycloak) |

**Getting the fat jar** (used by options A and B):

```bash
# Either build it from source (requires JDK 25):
./gradlew :sirix-rest-api:shadowJar
# -> bundles/sirix-rest-api/build/libs/sirix-rest-api-<version>-fat.jar

# ...or download it from a GitHub release:
# https://github.com/sirixdb/sirix/releases
```

The fat jar is compiled with `--enable-preview`, which pins it to the exact JDK
major version it was built with — **use a Java 25 JVM** (`java -version`).

---

## Option A — Dev mode, no Keycloak (fastest)

> Requires `auth.mode=none` support (any build of this tree newer than
> 1.0.0-alpha22). In this mode **authentication is disabled**: every request —
> with or without a token — runs as an admin user with all permissions. The
> server logs a loud warning. Local development only; never expose it.

### 1. Start the server

```bash
cat > sirix-dev-conf.json <<'EOF'
{
  "port": 9443,
  "use.http": true,
  "auth.mode": "none"
}
EOF

java -Xms256m -Xmx2g -XX:MaxDirectMemorySize=2g \
  --enable-preview --enable-native-access=ALL-UNNAMED \
  --add-modules=jdk.incubator.vector \
  --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -jar bundles/sirix-rest-api/build/libs/sirix-rest-api-*-fat.jar \
  -conf sirix-dev-conf.json
```

(`SIRIX_AUTH_MODE=none` as an environment variable works too and overrides the
config file.) Databases are stored under `$HOME/sirix-data/`; pass
`-Duser.home=/some/dir` to relocate them. The startup log shows:

```
WARN  io.sirix.rest.SirixVerticle - * AUTHENTICATION IS DISABLED (auth.mode=none). *
```

### 2. Check it's up

```bash
curl -s http://localhost:9443/health
```
```json
{"status":"UP"}
```

### 3. Create a database + resource (this is revision 1)

```bash
curl -s -X PUT http://localhost:9443/mydb/products \
  -H "Content-Type: application/json" \
  -d '[{"id":1,"name":"keyboard","price":49.9},{"id":2,"name":"mouse","price":19.9}]'
```
```json
[{"id":1,"name":"keyboard","price":49.9},{"id":2,"name":"mouse","price":19.9}]
```

### 4. Query it with JSONiq

```bash
curl -s -X POST http://localhost:9443/ \
  -H "Content-Type: application/json" \
  -d '{"query":"for $p in jn:doc('\''mydb'\'','\''products'\'')[] where $p.price lt 50 return $p.name"}'
```
```json
{"rest":[{"revisionNumber":1,"revisionTimestamp":"...","revision":{"name":"keyboard"}},
         {"revisionNumber":1,"revisionTimestamp":"...","revision":{"name":"mouse"}}]}
```

### 5. Commit a change (revision 2), then time travel

```bash
# JSONiq update queries auto-commit — this creates revision 2:
curl -s -X POST http://localhost:9443/ \
  -H "Content-Type: application/json" \
  -d '{"query":"insert json {\"name\": \"monitor\", \"id\": 3, \"price\": 199.0} into jn:doc('\''mydb'\'','\''products'\'')"}'

# Latest revision now has three products:
curl -s http://localhost:9443/mydb/products
# -> [{"id":1,...},{"id":2,...},{"name":"monitor","id":3,"price":199}]

# Revision 1 is still there, unchanged:
curl -s "http://localhost:9443/mydb/products?revision=1"
# -> [{"id":1,"name":"keyboard","price":49.9},{"id":2,"name":"mouse","price":19.9}]

# Or time travel inside a query — count items per revision:
curl -s -X POST http://localhost:9443/ -H "Content-Type: application/json" \
  -d '{"query":"count(jn:doc('\''mydb'\'','\''products'\'', 1)[])"}'   # -> {"rest":[2]}
curl -s -X POST http://localhost:9443/ -H "Content-Type: application/json" \
  -d '{"query":"count(jn:doc('\''mydb'\'','\''products'\'')[])"}'      # -> {"rest":[3]}
```

### 6. History and diff

```bash
curl -s http://localhost:9443/mydb/products/history
```
```json
{"history":[
  {"revision":2,"revisionTimestamp":"2026-06-11T13:15:23.069Z","author":"admin","commitMessage":""},
  {"revision":1,"revisionTimestamp":"2026-06-11T13:15:08.577Z","author":"admin","commitMessage":""}]}
```

```bash
curl -s "http://localhost:9443/mydb/products/diff?first-revision=1&second-revision=2"
```
```json
{"database":"mydb","resource":"products","old-revision":1,"new-revision":2,
 "diffs":[{"insert":{"nodeKey":10,"insertPositionNodeKey":6,
           "insertPosition":"asRightSibling","path":"/[2]","type":"jsonFragment"}}]}
```

That's the whole loop: create → query → commit → time travel → diff.

---

## Option B — Fat jar + Keycloak

The default and production configuration: all endpoints (except `/health` and
`/metrics`) require an OAuth2 bearer token.

### 1. Start Keycloak

The repository ships a pre-configured Keycloak (realm `sirixdb`, client `sirix`,
users `admin/admin` with full access and `viewer/viewer` read-only):

```bash
git clone https://github.com/sirixdb/sirix.git && cd sirix
docker compose up -d keycloak     # first run builds the image and takes ~1 minute
```

Wait until the realm answers:

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/realms/sirixdb   # -> 200
```

### 2. Start the SirixDB server

`bundles/sirix-rest-api/src/main/resources/sirix-conf.json` already points at
`http://localhost:8080/realms/sirixdb`:

```bash
java -Xms256m -Xmx2g -XX:MaxDirectMemorySize=2g \
  --enable-preview --enable-native-access=ALL-UNNAMED \
  --add-modules=jdk.incubator.vector \
  --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -jar bundles/sirix-rest-api/build/libs/sirix-rest-api-*-fat.jar \
  -conf bundles/sirix-rest-api/src/main/resources/sirix-conf.json
```

If Keycloak is not reachable the server now fails fast with an actionable error
(start Keycloak, or use `auth.mode=none`) instead of an opaque stack trace.

### 3. Get a token, then run the same flow as Option A

```bash
TOKEN=$(curl -s -X POST http://localhost:9443/token \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin","grant_type":"password"}' | jq -r .access_token)

curl -s -X PUT http://localhost:9443/mydb/products \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '[{"id":1,"name":"keyboard","price":49.9},{"id":2,"name":"mouse","price":19.9}]'
```

All requests are identical to Option A plus the
`-H "Authorization: Bearer $TOKEN"` header. Expected outputs are the same.
Tokens expire after a few minutes — re-run the `TOKEN=$(...)` line if you get
`401 Unauthorized`.

---

## Option C — Everything in Docker

```bash
git clone https://github.com/sirixdb/sirix.git && cd sirix
docker compose up -d              # builds + starts Keycloak and the server
curl -s http://localhost:9443/health    # -> {"status":"UP"} once healthy
```

Then continue exactly like [Option B step 3](#3-get-a-token-then-run-the-same-flow-as-option-a).

How the official Docker image differs from running the fat jar yourself:

- **Plain HTTP on port 9443** (the baked `sirix-docker-conf.json` sets
  `use.http: true`). No `-k`/TLS needed for local use; put a TLS-terminating
  proxy in front for anything public.
- **Keycloak hostname**: the baked config uses `http://keycloak:8080/...`
  (the compose service name), not `localhost`.
- **Memory knobs via env vars** instead of editing JVM flags:
  `SIRIX_XMS` (default `256m`), `SIRIX_XMX` (default `2g`),
  `SIRIX_MAX_DIRECT` (default `2g`), and `SIRIX_JAVA_OPTS` for any extra flags
  — e.g. `docker run -e SIRIX_XMX=8g ...`. The defaults fit a laptop; raise
  them for big imports or heavy concurrency.
- **Auth mode via env var**: `docker run -e SIRIX_AUTH_MODE=none -p 9443:9443
  sirixdb/sirix:latest` gives you Option A's no-Keycloak dev mode in Docker.
- **Data lives in `/opt/sirix/sirix-data`** inside the container — mount a
  volume (`-v sirix-data:/opt/sirix/sirix-data`) to persist it.

---

## Troubleshooting

- **`Unsupported class file major version` / preview-feature errors** — your
  `java` is not version 25. The fat jar requires the JDK major version it was
  built with.
- **Startup error mentioning OpenID Connect discovery** — Keycloak isn't up
  (or the `keycloak.url` is wrong). Start it (`docker compose up -d keycloak`)
  or use `auth.mode=none` for local development.
- **`401 Unauthorized`** — token missing or expired; fetch a fresh one from
  `POST /token`.
- **`403 Forbidden`** — the user lacks the role for that operation (`create`,
  `modify`, `view`, `delete` — global or per-database `<db>-<role>` roles).
- **Where is my data?** — under `$HOME/sirix-data/` (fat jar) or
  `/opt/sirix/sirix-data` (Docker).

For production deployment (TLS, real users, secret rotation, memory budgets),
see [`operations.md`](operations.md).
