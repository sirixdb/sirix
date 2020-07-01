package org.sirix.cli.parser

import kotlinx.cli.ArgType
import kotlinx.cli.optional
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.QueryCommand
import org.sirix.cli.commands.QueryCommandOptions
import java.time.LocalDateTime

class QuerySubCommand :
    AbstractArgSubCommand("query", "Querys the Database") {

    val revision by option(ArgType.Int, "revision", "r", "The revions to query")
    val revisionTimestamp by option(
        CliArgType.Timestamp(),
        "revision-timestamp",
        "rt",
        "The revision timestamp"
    )
    val startRevision by option(ArgType.Int, "start-revision", "sr", "The start revision");
    val endRevision by option(ArgType.Int, "end-revision", "er", "The end revision");
    val startRevisionTimestamp by option(
        CliArgType.Timestamp(),
        "start-revision-timestamp",
        "srt",
        "The start revision timestamp"
    )
    val endRevisionTimestamp by option(
        CliArgType.Timestamp(),
        "end-revision-timestamp",
        "ert",
        "The end revision timestamp"
    )
    val queryStr by argument(ArgType.String, description = "The Query String")

    override fun createCliCommand(options: CliOptions): CliCommand {
        return QueryCommand(
            options,
            QueryCommandOptions(
                queryStr,
                revision,
                revisionTimestamp,
                startRevision,
                endRevision,
                startRevisionTimestamp,
                endRevisionTimestamp
            )
        )
    }


}