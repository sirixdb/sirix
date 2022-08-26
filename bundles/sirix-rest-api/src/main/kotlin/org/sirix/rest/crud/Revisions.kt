package org.sirix.rest.crud

import org.sirix.api.NodeCursor
import org.sirix.api.NodeReadOnlyTrx
import org.sirix.api.NodeTrx
import org.sirix.api.ResourceSession
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

class Revisions {
    companion object {
        fun <R, W> getRevisionsToSerialize(
            startRevision: String?, endRevision: String?, startRevisionTimestamp: String?,
            endRevisionTimestamp: String?, manager: ResourceSession<R, W>, revision: String?,
            revisionTimestamp: String?
        ): IntArray
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            return when {
                startRevision != null && endRevision != null -> parseIntRevisions(startRevision, endRevision)
                startRevisionTimestamp != null && endRevisionTimestamp != null -> {
                    val tspRevisions = parseTimestampRevisions(startRevisionTimestamp, endRevisionTimestamp)
                    getRevisionNumbers(manager, tspRevisions).toList().toIntArray()
                }
                else -> getRevisionNumber(revision, revisionTimestamp, manager)
            }
        }

        fun <R, W> getRevisionNumber(rev: String?, revTimestamp: String?, manager: ResourceSession<R, W>): IntArray
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            return if (rev != null) {
                intArrayOf(rev.toInt())
            } else if (revTimestamp != null) {
                var revision = getRevisionNumber(manager, revTimestamp)
                if (revision == 0)
                    intArrayOf(++revision)
                else
                    intArrayOf(revision)
            } else {
                intArrayOf(manager.mostRecentRevisionNumber)
            }
        }

        private fun <R, W> getRevisionNumber(manager: ResourceSession<R, W>, revision: String): Int
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            val zdt = parseRevisionTimestamp(revision)
            return manager.getRevisionNumber(zdt.toInstant())
        }

        fun parseRevisionTimestamp(revision: String): ZonedDateTime {
            val revisionDateTime = try {
                LocalDateTime.parse(revision)
            } catch (e: DateTimeParseException) {
                LocalDateTime.parse(revision, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
            }
            return revisionDateTime.atZone(ZoneOffset.UTC)
        }

        private fun <R, W> getRevisionNumbers(
            manager: ResourceSession<R, W>,
            revisions: Pair<ZonedDateTime, ZonedDateTime>
        ): IntArray
                where R : NodeReadOnlyTrx,
                      R : NodeCursor,
                      W : NodeTrx,
                      W : NodeCursor {
            val zdtFirstRevision = revisions.first
            val zdtLastRevision = revisions.second
            var firstRevisionNumber = manager.getRevisionNumber(zdtFirstRevision.toInstant())
            var lastRevisionNumber = manager.getRevisionNumber(zdtLastRevision.toInstant())

            if (firstRevisionNumber == 0) ++firstRevisionNumber
            if (lastRevisionNumber == 0) ++lastRevisionNumber

            return (firstRevisionNumber..lastRevisionNumber).toSet().toIntArray()
        }

        private fun parseIntRevisions(startRevision: String, endRevision: String): IntArray {
            return (startRevision.toInt()..endRevision.toInt()).toSet().toIntArray()
        }

        private fun parseTimestampRevisions(
                startRevision: String,
                endRevision: String
        ): Pair<ZonedDateTime, ZonedDateTime> {
            val firstRevisionDateTime = parseRevisionTimestamp(startRevision)
            val lastRevisionDateTime = parseRevisionTimestamp(endRevision)

            return Pair(firstRevisionDateTime, lastRevisionDateTime)
        }
    }
}