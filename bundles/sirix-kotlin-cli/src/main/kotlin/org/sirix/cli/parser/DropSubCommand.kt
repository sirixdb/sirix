package org.sirix.cli.parser

import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.Drop

class DropSubCommand: AbstractUserCommand("drop", "Drop a Sirix DB") {

    override fun createCliCommand(options: CliOptions): CliCommand {
        return  Drop(options)
    }
}