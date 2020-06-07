package org.sirix.cli.parser

import kotlinx.cli.ArgType
import org.sirix.cli.commands.DataCommandOptions

abstract class AbstractDataCommand(name: String, actionDescription: String) : AbstractUserCommand(name, actionDescription) {

    val resource by option(ArgType.String, "resource", "r", "The name of the resource in the new Database. These parameter is required is data or datafile is set.")
    val data by option(ArgType.String, "data", "d", "Data to insert into the Database.")
    val datafile by option(ArgType.String, "datafile", "df", "File containing Data to insert into the Database.")
    val commitMessage by option(ArgType.String, "message", "m", "Use the given <msg> as the commit message.")

    var dataCommandOptions: DataCommandOptions? = null

    override fun execute() {
        super.execute()

        if (data != null || datafile != null) {
            if (resource == null) {
                throw  IllegalStateException("Expect data or datafile when resource ist set!")
            }
            dataCommandOptions = DataCommandOptions(resource ?: "",
                    data ?: "",
                    datafile ?: "",
                    commitMessage ?: "",
                    user)
        }
    }
}


