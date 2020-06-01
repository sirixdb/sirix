package org.sirix.cli.parser

import kotlinx.cli.Subcommand
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand

abstract class ArgSubCommand(name: String, actionDescription: String) : Subcommand(name, actionDescription) {

    abstract fun createCliCommand(options: CliOptions) : CliCommand

    abstract fun isValid() : Boolean

}