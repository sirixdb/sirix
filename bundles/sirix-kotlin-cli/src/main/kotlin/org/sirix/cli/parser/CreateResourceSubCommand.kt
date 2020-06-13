package org.sirix.cli.parser

import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.CreateResourceCommand

class CreateResourceSubCommand : AbstractUserCommand("create-resource", "Creation of a resource(s).") {

    val resourceNames by argument(CliArgType.Csv(), "Name(s) of the resource(s)")

    override fun createCliCommand(options: CliOptions): CliCommand {
        return CreateResourceCommand(options, resourceNames, user)
    }

}