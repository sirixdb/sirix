package org.sirix.cli.commands

import org.sirix.access.ResourceConfiguration
import org.sirix.api.Database
import org.sirix.cli.CliOptions
import kotlin.system.exitProcess


abstract class AbstractCreate(options: CliOptions, private val dataOptions: DataCommandOptions?) : CliCommand(options) {


    protected abstract fun createDatabase(): Boolean

    protected abstract fun insertData()

    @Throws(IllegalStateException::class)
    override fun execute() {

        if (createDatabase()) {
            cliPrinter.prnLnV("Database '${options.location}' created.")
        } else {
            cliPrinter.prnLnV("Database '${options.location}' not created!")
            exitProcess(1)
        }

        if (dataOptions != null) {
            insertData()
            cliPrinter.prnLnV("Data inserted")
        }
    }


    protected fun createOrRemoveAndCreateResource(database: Database<*>) {
        val resConfig = ResourceConfiguration.Builder(dataOptions!!.resourceName).build()
        if (!database.createResource(resConfig)) {
            database.removeResource(dataOptions.resourceName)
            database.createResource(resConfig)
        }
    }

}