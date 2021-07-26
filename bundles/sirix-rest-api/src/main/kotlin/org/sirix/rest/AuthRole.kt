package org.sirix.rest

enum class AuthRole(private val role: String) {
    CREATE("create"),
    MODIFY("modify"),
    VIEW("view"),
    DELETE("delete");

    fun keycloakRole() = "$role"
    fun databaseRole(database: String) = "$database-$role"
}
