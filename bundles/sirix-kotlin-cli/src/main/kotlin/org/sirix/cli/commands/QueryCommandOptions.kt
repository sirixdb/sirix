package org.sirix.cli.commands

import java.time.LocalDateTime

data class QueryCommandOptions(
    val queryStr: String,
    val revision: Int?,
    val revisionTimestamp: LocalDateTime?,
    val startRevision: Int?,
    val endRevision: Int?,
    val startRevisionTimestamp: LocalDateTime?,
    val endRevisionTimestamp: LocalDateTime?
) {

}