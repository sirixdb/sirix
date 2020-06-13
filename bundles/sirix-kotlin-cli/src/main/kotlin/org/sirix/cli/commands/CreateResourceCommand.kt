package org.sirix.cli.commands

import org.sirix.access.ResourceConfiguration
import org.sirix.access.User
import org.sirix.cli.CliOptions

class CreateResourceCommand(options: CliOptions, private val resourceNames: List<kotlin.String>, private val user: User?) : CliCommand(options) {

    override fun execute() {

        val database = openDatabase(user)

        database.use {
            resourceNames.forEach {
                if (database.createResource(ResourceConfiguration.Builder(it).build())) {
                    cliPrinter.prnLnV("Resource $it created")
                } else {
                    cliPrinter.prnLnV("Resource $it not created")
                }
            }
        }
    }

}