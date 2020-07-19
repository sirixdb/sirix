package org.sirix.cli.parser

import kotlinx.cli.ArgType
import kotlinx.cli.optional
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.QueryCommand
import org.sirix.cli.commands.QueryCommandOptions

class QuerySubCommand :
    AbstractUserCommand("query", "Querys the Database") {

    val resource by option(ArgType.String, "resource", "r", "The name of the resource.")
    val revision by option(ArgType.Int, "revision", "rev", "The revions to query")
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
    val nodeId by option(ArgType.String, "node-id", "nid", "The node id")
    val queryStr by argument(ArgType.String, description = "The Query String").optional()

    override fun createCliCommand(options: CliOptions): CliCommand {
        return QueryCommand(
            options,
            QueryCommandOptions(
                queryStr,
                resource,
                revision,
                revisionTimestamp,
                startRevision,
                endRevision,
                startRevisionTimestamp,
                endRevisionTimestamp,
                nodeId,
                user
            )
        )
    }


}
