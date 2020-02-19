package org.sirix.rest.crud

import org.sirix.api.NodeCursor
import org.sirix.api.NodeReadOnlyTrx
import org.sirix.api.NodeTrx
import org.sirix.api.ResourceManager
import java.time.LocalDateTime
import java.time.ZoneId

class Revisions {
    companion object {
        fun <R, W> getRevisionsToSerialize(
                startRevision: String?, endRevision: String?, startRevisionTimestamp: String?,
                endRevisionTimestamp: String?, manager: ResourceManager<R, W>, revision: String?,
                revisionTimestamp: String?
        ): Array<Int>
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            return when {
                startRevision != null && endRevision != null -> parseIntRevisions(startRevision, endRevision)
                startRevisionTimestamp != null && endRevisionTimestamp != null -> {
                    val tspRevisions = parseTimestampRevisions(startRevisionTimestamp, endRevisionTimestamp)
                    getRevisionNumbers(manager, tspRevisions).toList().toTypedArray()
                }
                else -> getRevisionNumber(revision, revisionTimestamp, manager)
            }
        }

        fun <R, W> getRevisionNumber(rev: String?, revTimestamp: String?, manager: ResourceManager<R, W>): Array<Int>
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            return if (rev != null) {
                arrayOf(rev.toInt())
            } else if (revTimestamp != null) {
                var revision = getRevisionNumber(manager, revTimestamp)
                if (revision == 0)
                    arrayOf(++revision)
                else
                    arrayOf(revision)
            } else {
                arrayOf(manager.mostRecentRevisionNumber)
            }
        }

        private fun <R, W> getRevisionNumber(manager: ResourceManager<R, W>, revision: String): Int
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            val revisionDateTime = LocalDateTime.parse(revision)
            val zdt = revisionDateTime.atZone(ZoneId.systemDefault())
            return manager.getRevisionNumber(zdt.toInstant())
        }

        private fun <R, W> getRevisionNumbers(
                manager: ResourceManager<R, W>,
                revisions: Pair<LocalDateTime, LocalDateTime>
        ): Array<Int>
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            val zdtFirstRevision = revisions.first.atZone(ZoneId.systemDefault())
            val zdtLastRevision = revisions.second.atZone(ZoneId.systemDefault())
            var firstRevisionNumber = manager.getRevisionNumber(zdtFirstRevision.toInstant())
            var lastRevisionNumber = manager.getRevisionNumber(zdtLastRevision.toInstant())

            if (firstRevisionNumber == 0) ++firstRevisionNumber
            if (lastRevisionNumber == 0) ++lastRevisionNumber

            return (firstRevisionNumber..lastRevisionNumber).toSet().toTypedArray()
        }

        private fun parseIntRevisions(startRevision: String, endRevision: String): Array<Int> {
            return (startRevision.toInt()..endRevision.toInt()).toSet().toTypedArray()
        }

        private fun parseTimestampRevisions(
                startRevision: String,
                endRevision: String
        ): Pair<LocalDateTime, LocalDateTime> {
            val firstRevisionDateTime = LocalDateTime.parse(startRevision)
            val lastRevisionDateTime = LocalDateTime.parse(endRevision)

            return Pair(firstRevisionDateTime, lastRevisionDateTime)
        }
    }
}