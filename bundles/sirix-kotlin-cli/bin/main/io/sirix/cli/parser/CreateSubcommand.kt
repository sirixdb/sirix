package io.sirix.cli.parser

import kotlinx.cli.ArgType
import io.sirix.access.DatabaseType
import io.sirix.cli.commands.CliCommand
import io.sirix.cli.commands.JsonCreate
import io.sirix.cli.commands.XmlCreate

class CreateSubcommand : AbstractDataCommand("create", "Create a Sirix DB") {
    private val type by argument(ArgType.Choice(listOf("xml", "json")), "The Type of the Database")

    private var dataBasetype: DatabaseType? = null

    override fun execute() {
        super.execute()
        dataBasetype = DatabaseType.valueOf(type.uppercase())
    }

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
        return when (dataBasetype) {
            DatabaseType.XML -> XmlCreate(options, dataCommandOptions)
            DatabaseType.JSON -> JsonCreate(options, dataCommandOptions)
            else -> throw IllegalStateException("Unknown DatabaseType '$type'!")
        }
    }
}
