package io.sirix.rest.crud.json

import io.sirix.access.Databases
import io.sirix.api.json.JsonResourceSession
import io.sirix.query.json.BasicJsonDBStore
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Answers
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.nio.file.Path

/**
 * REST-213: the paginated JSON read continues after `startNodeKey` and advances with it, and the
 * retired name `lastTopLevelNodeKey` is refused with a 400 naming its replacement instead of being
 * ignored (which served the first page for every request, so pagination never advanced).
 */
@Tag("offline")
@DisplayName("Retired pagination parameter lastTopLevelNodeKey")
class RetiredPaginationParameterTest {
    @TempDir
    lateinit var directory: Path

    private val pageSize = 5
    private val elementCount = 20

    @Test
    @DisplayName("startNodeKey pages through every element exactly once, in order")
    fun startNodeKeyAdvancesTheCursor() {
        withPaginatedResource { handler, session ->
            val keys = mutableListOf<Long>()
            var cursor: Long? = null
            var pages = 0
            while (true) {
                val page = pageKeys(handler, session, cursor, PARAM_NEW)
                if (page.isEmpty()) break
                assertTrue(page.size <= pageSize)
                keys += page
                cursor = page.last()
                pages++
                assertTrue(pages <= elementCount, "the cursor did not advance")
            }
            assertEquals(elementCount / pageSize, pages)
            assertEquals(elementCount, keys.size)
            assertEquals(keys.sorted(), keys)
            assertEquals(elementCount, keys.toSet().size)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["0", "7", "not-a-number", ""])
    @DisplayName("the retired name alone is refused with 400 naming startNodeKey, whatever its value")
    fun retiredNameIsRefused(value: String) {
        withPaginatedResource { handler, session ->
            val failure = assertThrows<HttpException> {
                body(handler, session, mapOf(PARAM_PAGE to "$pageSize", PARAM_OLD to value))
            }
            assertEquals(400, failure.statusCode)
            assertTrue(failure.payload.contains(PARAM_NEW), failure.payload)
            assertTrue(failure.payload.contains(PARAM_OLD), failure.payload)
        }
    }

    @Test
    @DisplayName("the retired name together with startNodeKey is refused too, not resolved to either")
    fun retiredNameWithNewNameIsRefused() {
        withPaginatedResource { handler, session ->
            val first = pageKeys(handler, session, null, PARAM_NEW)
            val failure = assertThrows<HttpException> {
                body(
                    handler,
                    session,
                    mapOf(PARAM_PAGE to "$pageSize", PARAM_NEW to "${first.last()}", PARAM_OLD to "${first.last()}")
                )
            }
            assertEquals(400, failure.statusCode)
        }
    }

    @Test
    @DisplayName("the retired name is refused on a read without pagination as well")
    fun retiredNameIsRefusedWithoutPagination() {
        withPaginatedResource { handler, session ->
            val failure = assertThrows<HttpException> { body(handler, session, mapOf(PARAM_OLD to "3")) }
            assertEquals(400, failure.statusCode)
        }
    }

    @Test
    @DisplayName("a read with neither pagination name still serves the first page")
    fun noCursorServesTheFirstPage() {
        withPaginatedResource { handler, session ->
            val page = pageKeys(handler, session, null, PARAM_NEW)
            assertEquals(pageSize, page.size)
            assertEquals(page.sorted(), page)
        }
    }

    private fun pageKeys(handler: JsonGet, session: JsonResourceSession, cursor: Long?, name: String): List<Long> {
        val params = mutableMapOf(PARAM_PAGE to "$pageSize", "withMetaData" to "nodeKeyAndChildCount")
        if (cursor != null) params[name] = "$cursor"
        val json = JsonObject(body(handler, session, params))
        val children = json.getJsonArray("value")
        assertNotNull(children)
        return children.map { (it as JsonObject).getJsonObject("metadata").getLong("nodeKey") }
    }

    private fun body(handler: JsonGet, session: JsonResourceSession, params: Map<String, String>): String {
        val ctx = Mockito.mock(RoutingContext::class.java, Mockito.withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
        Mockito.`when`(ctx.queryParam(ArgumentMatchers.anyString())).thenAnswer { invocation ->
            params[invocation.getArgument<String>(0)]?.let { listOf(it) } ?: emptyList<String>()
        }
        return JsonGetTestBinding.serializedBody(handler, session, intArrayOf(1), PAGINATED_NODE_ID, ctx)
            .toString()
    }

    private fun withPaginatedResource(action: (JsonGet, JsonResourceSession) -> Unit) {
        val storePath = directory.resolve("store")
        val elements = (1..elementCount).joinToString(",") { """{"idx":$it}""" }
        try {
            BasicJsonDBStore.newBuilder().location(storePath).build().use { store ->
                store.create("pagination", "resource1", "[$elements]").close()
                val authz = Mockito.mock(AuthorizationProvider::class.java)
                val handler = JsonGet(storePath, Mockito.mock(OAuth2Auth::class.java), authz)
                Databases.openJsonDatabase(storePath.resolve("pagination")).use { database ->
                    database.beginResourceSession("resource1").use { session ->
                        action(handler, session)
                    }
                }
            }
        } finally {
            Databases.removeDatabase(storePath.resolve("pagination"))
        }
    }

    private companion object {
        const val PARAM_NEW = "startNodeKey"
        const val PARAM_OLD = "lastTopLevelNodeKey"
        const val PARAM_PAGE = "nextTopLevelNodes"

        // The top-level array is the document root's first child.
        const val PAGINATED_NODE_ID = 1L
    }
}
