package io.sirix.cli.parser

import io.sirix.cli.commands.CliCommand
import io.sirix.cli.commands.DropResource

class DropResourceSubCommand : AbstractUserCommand("drop-resource", "Dropping of a resource(s).") {
    val resourceNames by argument(CliArgType.Csv(), "Name(s) of the resource(s)")

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
        return DropResource(options, resourceNames, user)
    }
}