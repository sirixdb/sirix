package io.sirix.cli.parser

import io.sirix.cli.commands.CliCommand
import io.sirix.cli.commands.CreateResource

class CreateResourceSubCommand : AbstractUserCommand("create-resource", "Creation of a resource(s).") {

    private val resourceNames by argument(CliArgType.Csv(), "Name(s) of the resource(s)")

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
        return CreateResource(options, resourceNames, user)
    }
}