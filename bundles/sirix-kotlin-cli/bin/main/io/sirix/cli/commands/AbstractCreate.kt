package io.sirix.cli.commands

import io.sirix.access.ResourceConfiguration
import io.sirix.api.Database
import kotlin.system.exitProcess


abstract class AbstractCreate(options: io.sirix.cli.CliOptions, private val dataOptions: DataCommandOptions?) : CliCommand(options) {


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