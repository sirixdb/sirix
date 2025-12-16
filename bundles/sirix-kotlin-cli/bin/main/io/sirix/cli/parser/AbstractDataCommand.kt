package io.sirix.cli.parser

import kotlinx.cli.ArgType
import io.sirix.cli.commands.DataCommandOptions

abstract class AbstractDataCommand(name: String, actionDescription: String) :
    AbstractUserCommand(name, actionDescription) {

    val resource by option(
        ArgType.String,
        "resource",
        "r",
        "The name of the resource in the Database. These parameter is required if data or datafile is set."
    )
    private val data by option(ArgType.String, "data", "d", "Data to insert into the Database.")
    private val datafile by option(ArgType.String, "datafile", "df", "File containing Data to insert into the Database.")
    private val commitMessage by option(ArgType.String, "message", "m", "Use the given <msg> as the commit message.")

    var dataCommandOptions: DataCommandOptions? = null

    override fun execute() {
        super.execute()

        if (data != null || datafile != null) {
            if (resource == null) {
                throw  IllegalStateException("Expect data or datafile when resource ist set!")
            }
            dataCommandOptions = DataCommandOptions(
                resource ?: "",
                data ?: "",
                datafile ?: "",
                commitMessage ?: "",
                user
            )
        }
    }
}


