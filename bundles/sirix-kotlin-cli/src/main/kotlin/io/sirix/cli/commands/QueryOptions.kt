package io.sirix.cli.commands

import io.sirix.access.User
import io.sirix.cli.MetaDataEnum
import java.time.LocalDateTime

data class QueryOptions(
    val queryStr: String?,
    val resource: String,
    val revision: Int?,
    val revisionTimestamp: LocalDateTime?,
    val startRevision: Int?,
    val endRevision: Int?,
    val startRevisionTimestamp: LocalDateTime?,
    val endRevisionTimestamp: LocalDateTime?,
    val nodeId: Long?,
    val nextTopLevelNodes: Int?,
    val lastTopLevelNodeKey: Long?,
    val startResultSeqIndex: Long?,
    val endResultSeqIndex: Long?,
    val maxLevel: Long?,
    val metaData: MetaDataEnum,
    val prettyPrint: Boolean,
    val user: User?
) {
    fun hasQueryStr(): Boolean = !queryStr.isNullOrEmpty()
}
