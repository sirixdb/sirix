package org.sirix.cli.commands

import org.sirix.access.Databases
import org.sirix.cli.CliOptions

class Drop(options: CliOptions): CliCommand(options) {

    override fun execute() {
        Databases.removeDatabase(path())
        cliPrinter.prnLnV("Database dropped")
    }
}