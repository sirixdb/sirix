package io.sirix.cli.commands

import io.sirix.access.User

class DropResource(options: io.sirix.cli.CliOptions, private val resourceNames: List<String>, private val user: User?): CliCommand(options) {

    override fun execute() {
        var database = openDatabase(user)

        try {
            resourceNames.forEach {
                database = database.removeResource(it)
                cliPrinter.prnLnV("Resource $it removed")
            }
        } finally {
            database.close()
        }
    }
}