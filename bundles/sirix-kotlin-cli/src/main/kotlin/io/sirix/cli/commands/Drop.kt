package io.sirix.cli.commands

import io.sirix.access.Databases

class Drop(options: io.sirix.cli.CliOptions): CliCommand(options) {

    override fun execute() {
        Databases.removeDatabase(path())
        cliPrinter.prnLnV("Database dropped")
    }
}