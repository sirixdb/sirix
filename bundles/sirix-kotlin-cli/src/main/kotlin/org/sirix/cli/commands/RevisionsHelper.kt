package org.sirix.cli.commands

import org.sirix.api.NodeCursor
import org.sirix.api.NodeReadOnlyTrx
import org.sirix.api.NodeTrx
import org.sirix.api.ResourceManager
import java.time.LocalDateTime
import java.time.ZoneId

class RevisionsHelper {
    companion object {
        fun <R, W> getRevisionsToSerialize(
            startRevision: Int?, endRevision: Int?, startRevisionTimestamp: LocalDateTime?,
            endRevisionTimestamp: LocalDateTime?, manager: ResourceManager<R, W>, revision: Int?,
            revisionTimestamp: LocalDateTime?
        ): Array<Int>
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            return when {
                startRevision != null && endRevision != null -> (startRevision..endRevision).toSet().toTypedArray()
                startRevisionTimestamp != null && endRevisionTimestamp != null -> {
                    val tspRevisions = Pair(startRevisionTimestamp, endRevisionTimestamp)
                    getRevisionNumbers(
                        manager,
                        tspRevisions
                    ).toList().toTypedArray()
                }
                else -> getRevisionNumber(
                    revision,
                    revisionTimestamp,
                    manager
                )
            }
        }

        fun <R, W> getRevisionNumber(
            rev: Int?,
            revTimestamp: LocalDateTime?,
            manager: ResourceManager<R, W>
        ): Array<Int>
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            return if (rev != null) {
                arrayOf(rev.toInt())
            } else if (revTimestamp != null) {
                var revision = getRevisionNumber(
                    manager,
                    revTimestamp
                )
                if (revision == 0)
                    arrayOf(++revision)
                else
                    arrayOf(revision)
            } else {
                arrayOf(manager.mostRecentRevisionNumber)
            }
        }

        private fun <R, W> getRevisionNumber(manager: ResourceManager<R, W>, revision: LocalDateTime): Int
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            val zdt = revision.atZone(ZoneId.systemDefault())
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
