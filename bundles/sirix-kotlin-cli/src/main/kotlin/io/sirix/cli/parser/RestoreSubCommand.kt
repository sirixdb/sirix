package io.sirix.cli.parser

import kotlinx.cli.ArgType
import io.sirix.cli.commands.CliCommand
import io.sirix.cli.commands.Restore

class RestoreSubCommand :
    AbstractArgSubCommand("restore", "Restore a Sirix DB backup (the --location option points at the backup)") {

    private val target by argument(
        ArgType.String,
        "target",
        "The directory the database is restored to (must not exist yet or be empty)"
    )

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
        return Restore(options, target)
    }
}
