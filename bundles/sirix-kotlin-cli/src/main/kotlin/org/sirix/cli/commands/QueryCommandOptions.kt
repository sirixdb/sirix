package org.sirix.cli.commands

import org.sirix.access.User
import java.time.LocalDateTime

data class QueryCommandOptions(
    val queryStr: String?,
    val resource: String?,
    val revision: Int?,
    val revisionTimestamp: LocalDateTime?,
    val startRevision: Int?,
    val endRevision: Int?,
    val startRevisionTimestamp: LocalDateTime?,
    val endRevisionTimestamp: LocalDateTime?,
    val nodeId: String?,
    val user: User?
) {

    fun hasQueryStr(): Boolean = queryStr != null && queryStr.isNotEmpty()

}
