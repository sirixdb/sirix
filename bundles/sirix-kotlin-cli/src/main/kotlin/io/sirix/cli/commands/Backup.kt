package io.sirix.cli.commands

import io.sirix.backup.BackupManager
import java.nio.file.Paths

/**
 * Creates a consistent online backup of the Sirix database at the `--location` option in
 * [targetPath]. Per-resource consistency is guaranteed by [BackupManager] (each resource's
 * writer lock is held while its files are copied).
 */
class Backup(options: io.sirix.cli.CliOptions, private val targetPath: String) : CliCommand(options) {

    override fun execute() {
        val summary = BackupManager.backupDatabase(path(), Paths.get(targetPath))
        cliPrinter.prnLn("Database backed up to '${summary.targetDir()}'.")
        printBackupSummary(summary, cliPrinter)
    }
}

/** Prints the per-resource detail of a backup/restore summary (verbose only). */
internal fun printBackupSummary(summary: BackupManager.BackupSummary, cliPrinter: io.sirix.cli.CliPrinter) {
    cliPrinter.prnLnV("Resources: ${summary.resourcesCopied()}, total bytes: ${summary.totalBytesCopied()}")
    summary.resources().forEach {
        cliPrinter.prnLnV("  - ${it.resourceName()}: revision ${it.mostRecentRevision()}, ${it.bytesCopied()} bytes")
    }
}
