package org.sirix.cli.parser

import kotlinx.cli.ArgType
import org.sirix.access.DatabaseType
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.Create

class CreateSubcommand : ArgSubCommand("create", "Create a Sirix DB") {
    val type by argument(ArgType.Choice(listOf("xml", "json")), "The Type of the Database")
    val data by option(ArgType.String, "data ", "d", "Data to insert into the Database")
    val datafile by option(ArgType.String, "datafile", "df", "File containing Data to insert into sthe Database")


    var dataBasetype: DatabaseType? = null

    override fun execute() {
        dataBasetype = DatabaseType.valueOf(type.toUpperCase())
    }

    override fun isValid(): Boolean {
        return dataBasetype != null
    }

    override fun createCliCommand(options: CliOptions) : CliCommand {
        return Create(options, dataBasetype!!)
    }
}
