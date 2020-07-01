package org.sirix.cli.commands

import org.sirix.access.DatabaseType.JSON
import org.sirix.access.DatabaseType.XML
import org.sirix.access.Databases
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.json.JsonQueryCommand
import org.sirix.cli.commands.xml.XmlQueryCommand

class QueryCommand(options: CliOptions, private val queryCommandOptions: QueryCommandOptions) : CliCommand(options) {

    override fun execute() {
        when (Databases.getDatabaseType(path())) {
            JSON -> JsonQueryCommand(options, queryCommandOptions).execute()
            XML -> XmlQueryCommand(options, queryCommandOptions).execute()
            else -> throw IllegalStateException("Unknown Database Type!")
        }
    }
}