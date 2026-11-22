package io.sirix.rest.crud

import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.api.Database
import io.sirix.api.json.JsonResourceSession
import io.sirix.io.StorageType
import io.sirix.service.json.shredder.JsonShredder
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Path

/**
 * REST-210 (#1477): PUT creates a missing resource with the CREATE role the route checks, but
 * replacing an existing one discards its history and additionally needs MODIFY and DELETE on the
 * database, checked before anything is removed.
 */
@Tag("offline")
class CreateOrReplaceResourceAuthorizationTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @ValueSource(strings = ["create", "create|modify", "create|delete", "auth-create|auth-modify"])
    fun replacingWithoutModifyAndDeleteIsRefusedAndKeepsTheResource(roles: String) {
        withDatabase { database ->
            val refused = assertThrows(HttpException::class.java) {
                AbstractCreateHandler.createOrReplaceResource(database, config("doc"), "doc", userWith(roles), "auth")
            }
            assertEquals(403, refused.statusCode)
            assertEquals(setOf("doc"), resources(database))
            assertEquals(2, revisions(database, "doc"), "a refused replacement must keep the resource's history")
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["create|modify|delete", "auth-create|auth-modify|auth-delete"])
    fun replacingWithModifyAndDeleteStartsANewHistory(roles: String) {
        withDatabase { database ->
            AbstractCreateHandler.createOrReplaceResource(database, config("doc"), "doc", userWith(roles), "auth")
            assertEquals(setOf("doc"), resources(database))
            assertEquals(0, revisions(database, "doc"), "the replacement is a new, still empty resource")
        }
    }

    @Test
    fun creatingAMissingResourceNeedsNothingBeyondTheRoutesCreateRole() {
        withDatabase { database ->
            AbstractCreateHandler.createOrReplaceResource(database, config("fresh"), "fresh", userWith("create"), "auth")
            assertEquals(setOf("doc", "fresh"), resources(database))
            assertEquals(2, revisions(database, "doc"))
        }
    }

    /** Database `auth` holding resource `doc` with two revisions. */
    private fun withDatabase(body: (Database<JsonResourceSession>) -> Unit) {
        val path = directory.resolve("auth")
        Databases.createJsonDatabase(DatabaseConfiguration(path))
        Databases.openJsonDatabase(path).use { database ->
            database.createResource(config("doc"))
            database.beginResourceSession("doc").use { session ->
                session.beginNodeTrx().use { writer ->
                    writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"v\":1}"))
                    writer.moveToDocumentRoot()
                    writer.moveToFirstChild()
                    writer.moveToFirstChild()
                    writer.moveToFirstChild()
                    writer.setNumberValue(2)
                    writer.commit()
                }
            }
            body(database)
        }
    }

    private fun config(name: String): ResourceConfiguration =
        ResourceConfiguration.newBuilder(name).storageType(StorageType.FILE_CHANNEL).build()

    private fun resources(database: Database<JsonResourceSession>): Set<String> =
        database.listResources().map { it.fileName.toString() }.toSet()

    private fun revisions(database: Database<JsonResourceSession>, name: String): Int =
        database.beginResourceSession(name).use { it.mostRecentRevisionNumber }

    private fun userWith(roles: String): User {
        val user = User.fromName("replace-test")
        user.authorizations().put("test", roles.split('|').map { RoleBasedAuthorization.create(it) }.toSet())
        return user
    }
}
