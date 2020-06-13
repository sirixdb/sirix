package org.sirix.cli.parser

import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.DropCommand

class DropSubCommand: AbstractUserCommand("drop", "Drop a Sirix DB") {

    override fun createCliCommand(options: CliOptions): CliCommand {
        return  DropCommand(options)
    }
}