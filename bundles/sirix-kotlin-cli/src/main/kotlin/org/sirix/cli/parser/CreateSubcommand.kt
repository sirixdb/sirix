package org.sirix.cli.parser

import kotlinx.cli.ArgType
import org.sirix.access.DatabaseType
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.JsonCreate
import org.sirix.cli.commands.XmlCreate

class CreateSubcommand: AbstractDataCommand("create", "Create a Sirix DB") {
    val type by argument(ArgType.Choice(listOf("xml", "json")), "The Type of the Database")

    var dataBasetype: DatabaseType? = null

    override fun execute() {
        super.execute()
        dataBasetype = DatabaseType.valueOf(type.toUpperCase())
    }

    override fun createCliCommand(options: CliOptions) : CliCommand {
        return when(dataBasetype) {
            DatabaseType.XML -> XmlCreate(options, dataCommandOptions)
            DatabaseType.JSON -> JsonCreate(options, dataCommandOptions)
            else -> throw IllegalStateException("Unknown DatabaseType '$type'!")
        }

    }
}
