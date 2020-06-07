package org.sirix.cli.commands

import org.sirix.cli.CliOptions
import org.sirix.cli.CliPrinter

abstract class CliCommand(protected val options: CliOptions) {
    protected val cliPrinter = CliPrinter(options.verbose)

    abstract fun execute()

}