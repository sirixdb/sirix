package org.sirix.rest

enum class AuthRole(role: String) {
    CREATE("create"),
    MODIFY("modify"),
    VIEW("view"),
    DELETE("delete");

    fun keycloakRole() = "realm:$this"
    fun databaseRole(database: String) = "realm:$database-$this"
}