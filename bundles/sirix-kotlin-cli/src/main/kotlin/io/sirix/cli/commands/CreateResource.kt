package io.sirix.cli.commands

import io.sirix.access.ResourceConfiguration
import io.sirix.access.User

class CreateResource(options: io.sirix.cli.CliOptions, private val resourceNames: List<String>, private val user: User?) : CliCommand(options) {

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