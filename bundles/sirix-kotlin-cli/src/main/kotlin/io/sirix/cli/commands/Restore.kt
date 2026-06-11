package io.sirix.cli.commands

import io.sirix.backup.BackupManager
import java.nio.file.Paths

/**
 * Restores the backup at the `--location` option to [targetPath] (a new database directory) and
 * verifies the result by opening every restored resource read-only. On verification failure the
 * partial target is deleted.
 */
class Restore(options: io.sirix.cli.CliOptions, private val targetPath: String) : CliCommand(options) {

    override fun execute() {
        val summary = BackupManager.restoreDatabase(path(), Paths.get(targetPath))
        cliPrinter.prnLn("Backup restored and verified at '${summary.targetDir()}'.")
        printBackupSummary(summary, cliPrinter)
    }
}
