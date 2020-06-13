package org.sirix.cli.parser

import kotlinx.cli.ArgType
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.DumpResourceHistoryCommand

class DumpResourceHistorySubCommand : AbstractUserCommand("resource-history", "Prints out the History of a resource") {

    val resourceName by argument(ArgType.String, "resource",  "The Name of the Resource")

    override fun createCliCommand(options: CliOptions): CliCommand {
        return DumpResourceHistoryCommand(options, resourceName, user)
    }

}