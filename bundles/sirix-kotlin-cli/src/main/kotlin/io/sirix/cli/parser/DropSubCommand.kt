package io.sirix.cli.parser

import io.sirix.cli.commands.CliCommand
import io.sirix.cli.commands.Drop

class DropSubCommand: AbstractUserCommand("drop", "Drop a Sirix DB") {

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
        return  Drop(options)
    }
}