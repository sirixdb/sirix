package org.sirix.cli.commands

import org.sirix.access.User
import org.sirix.cli.CliOptions

class DropResource(options: CliOptions, private val resourceNames: List<kotlin.String>, private val user: User?): CliCommand(options) {

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