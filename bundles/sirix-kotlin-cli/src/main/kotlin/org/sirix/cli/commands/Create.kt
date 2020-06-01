package org.sirix.cli.commands

import org.sirix.access.DatabaseConfiguration
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.cli.CliOptions
import java.nio.file.Paths
import kotlin.system.exitProcess

class Create(options: CliOptions, private val type: DatabaseType) : CliCommand(options) {


    @Throws(IllegalStateException::class)
    override fun execute() {

        val path = Paths.get(options.file)

        var isValid = when(type) {
            DatabaseType.XML -> Databases.createXmlDatabase(DatabaseConfiguration(path))
            DatabaseType.JSON -> Databases.createJsonDatabase(DatabaseConfiguration(path))
            else -> throw IllegalStateException("Unknown DatabaseType '$type'!")
        }

        if(isValid) {
            cliPrinter.prnLnV("Database '${options.file}' type '$type' created." )
        } else {
            cliPrinter.prnLnV("Database '${options.file}' type '$type' not created!" )
            exitProcess(1)
        }


    }


}