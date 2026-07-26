package io.sirix.rest.crud

import io.sirix.api.NodeCursor
import io.sirix.api.NodeReadOnlyTrx
import io.sirix.api.NodeTrx
import io.sirix.api.ResourceSession
import java.time.LocalDateTime
import java.time.OffsetDateTime
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
                // Validate instead of bare toInt(): "?revision=abc" previously threw a
                // NumberFormatException mapped to a generic 500, and out-of-range numbers
                // surfaced as low-level transaction errors. Both are client errors (400).
                val revisionNumber = requireIntParam("revision", rev)
                if (revisionNumber < 1 || revisionNumber > manager.mostRecentRevisionNumber) {
                    throw IllegalArgumentException(
                        "revision $revisionNumber out of range [1, ${manager.mostRecentRevisionNumber}]"
                    )
                }
                intArrayOf(revisionNumber)
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
            // Try parsing as ZonedDateTime first (handles timestamps with full timezone like 2024-01-09T15:30:00Z[UTC])
            try {
                return ZonedDateTime.parse(revision)
            } catch (_: DateTimeParseException) {
                // Continue to try other formats
            }
            
            // Try parsing as OffsetDateTime (handles timestamps with timezone offset like 2024-01-09T15:30:00+01:00)
            try {
                return OffsetDateTime.parse(revision).toZonedDateTime()
            } catch (_: DateTimeParseException) {
                // Continue to try other formats
            }
            
            // Try parsing as LocalDateTime (ISO 8601 without timezone: 2024-01-09T15:30:00 or 2024-01-09T15:30)
            val revisionDateTime = try {
                LocalDateTime.parse(revision)
            } catch (_: DateTimeParseException) {
                // Try with space separator instead of T: 2024-01-09 15:30:00.SSS
                try {
                    LocalDateTime.parse(revision, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
                } catch (_: DateTimeParseException) {
                    // Try without milliseconds: 2024-01-09 15:30:00
                    try {
                        LocalDateTime.parse(revision, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    } catch (_: DateTimeParseException) {
                        // Try without seconds: 2024-01-09 15:30
                        LocalDateTime.parse(revision, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))
                    }
                }
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

            // Validate ordering like parseIntRevisions: an inverted timestamp range produced an
            // EMPTY array, and downstream revisions[0] turned that into a 500.
            if (lastRevisionNumber < firstRevisionNumber) {
                throw IllegalArgumentException(
                    "invalid revision range [$firstRevisionNumber, $lastRevisionNumber]: " +
                            "end-revision-timestamp resolves to a revision before start-revision-timestamp"
                )
            }

            return ascendingRevisionArray(firstRevisionNumber, lastRevisionNumber)
        }

        /** An ascending int range is already distinct and ordered — no boxing/Set detour needed. */
        private fun ascendingRevisionArray(start: Int, end: Int): IntArray =
            if (end < start) IntArray(0) else IntArray(end - start + 1) { start + it }

        private fun parseIntRevisions(startRevision: String, endRevision: String): IntArray {
            // Validate: non-numeric input 500'd, an inverted range silently produced an empty set,
            // and a huge end allocated an enormous IntArray. The range is clamped by the CALLER's
            // semantics (revisions beyond mostRecent fail downstream), so just bound the basics.
            val start = requireIntParam("start-revision", startRevision)
            val end = requireIntParam("end-revision", endRevision)
            if (start < 1 || end < start) {
                throw IllegalArgumentException("invalid revision range [$start, $end]")
            }
            if (end - start > 1_000_000) {
                throw IllegalArgumentException("revision range too large: ${end - start + 1} revisions")
            }
            return ascendingRevisionArray(start, end)
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