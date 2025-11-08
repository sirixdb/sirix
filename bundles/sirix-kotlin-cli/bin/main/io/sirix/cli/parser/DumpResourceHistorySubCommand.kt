package io.sirix.cli.parser

import kotlinx.cli.ArgType
import io.sirix.cli.commands.CliCommand
import io.sirix.cli.commands.DumpResourceHistory

class DumpResourceHistorySubCommand : AbstractUserCommand("resource-history", "Prints out the History of a resource") {

    val resourceName by argument(ArgType.String, "resource",  "The Name of the Resource")

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
        return DumpResourceHistory(options, resourceName, user)
    }
}