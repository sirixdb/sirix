package io.sirix.cli.commands

import io.sirix.backup.BackupManager.BackupSummary
import io.sirix.backup.BackupManager.ResourceSummary
import io.sirix.cli.CliPrinter
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * TXN-615 clause 10: omitted volatile resources must be visible even without --verbose.
 * TXN-613: so must the hidden derived resources a backup or restore carried.
 */
internal class BackupTest {
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun reportsEveryOmittedResource(verbose: Boolean) {
        val summary = BackupSummary(Path.of("source"), Path.of("backup"),
            listOf(ResourceSummary("durable", 2, 123)), emptyList(), 123, listOf("volatile-a", "volatile-b"))
        val expected = mutableListOf<String>()
        if (verbose) {
            expected.add("Resources: 1, total bytes: 123")
            expected.add("  - durable: revision 2, 123 bytes")
        }
        expected.add("Not backed up (in-memory resources): volatile-a, volatile-b")
        assertEquals(expected, output(summary, verbose))
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun doesNotReportOmissionsForPersistentOnlyBackup(verbose: Boolean) {
        val summary = BackupSummary(Path.of("source"), Path.of("backup"), emptyList(), 0)
        assertEquals(if (verbose) listOf("Resources: 0, total bytes: 0") else emptyList<String>(),
            output(summary, verbose))
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun reportsEveryDerivedResource(verbose: Boolean) {
        val summary = BackupSummary(Path.of("source"), Path.of("backup"),
            listOf(ResourceSummary("durable", 2, 123)), listOf(ResourceSummary("durable${'$'}derived", 1, 77)), 200,
            emptyList())
        val expected = mutableListOf<String>()
        if (verbose) {
            expected.add("Resources: 1, total bytes: 200")
            expected.add("  - durable: revision 2, 123 bytes")
        }
        expected.add("Derived resources (temporal aggregates): durable${'$'}derived")
        if (verbose) {
            expected.add("  - durable${'$'}derived: revision 1, 77 bytes")
        }
        assertEquals(expected, output(summary, verbose))
    }

    private fun output(summary: BackupSummary, verbose: Boolean): List<String> {
        val previous = System.out
        val bytes = ByteArrayOutputStream()
        try {
            PrintStream(bytes, true, StandardCharsets.UTF_8).use { printer ->
                System.setOut(printer)
                printBackupSummary(summary, CliPrinter(verbose))
            }
        } finally {
            System.setOut(previous)
        }
        return bytes.toString(StandardCharsets.UTF_8).lineSequence().filter { it.isNotEmpty() }.toList()
    }
}
