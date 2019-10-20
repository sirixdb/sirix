package org.sirix.rest

enum class AuthRole(private val role: String) {
    CREATE("create"),
    MODIFY("modify"),
    VIEW("view"),
    DELETE("delete");

    fun keycloakRole() = "realm:$role"
    fun databaseRole(database: String) = "realm:$database-$role"
}