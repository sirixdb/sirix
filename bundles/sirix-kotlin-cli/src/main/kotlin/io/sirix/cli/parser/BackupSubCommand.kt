package io.sirix.cli.parser

import kotlinx.cli.ArgType
import io.sirix.cli.commands.Backup
import io.sirix.cli.commands.CliCommand

class BackupSubCommand : AbstractArgSubCommand("backup", "Create a consistent backup copy of a Sirix DB") {

    private val target by argument(
        ArgType.String,
        "target",
        "The directory the backup is written to (must not exist yet or be empty)"
    )

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
        return Backup(options, target)
    }
}
