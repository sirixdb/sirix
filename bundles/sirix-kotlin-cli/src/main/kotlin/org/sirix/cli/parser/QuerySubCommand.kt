package org.sirix.cli.parser

import kotlinx.cli.ArgType
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.QueryCommand
import java.time.LocalDateTime

class QuerySubCommand(name: String, actionDescription: String) : AbstractArgSubCommand(name, actionDescription) {

    val revision: Int by option(ArgType.Int, "revision", "r", "The revions to query")
    val revisionTimestamp: LocalDateTime by option(
        CliArgType.Timestamp
        "revision-timestamp",
        "rt",
        "The revision timestamp"
    )
    val startRevision: Int by option("start-revision", "sr", "The start revision");
    val endRevision: Int by option("end-revision", "er", "The end revision");
    val startRevisionTimestamp: LocalDateTime by option(
        CliArgType.Timestamp
        "start-revision-timestamp",
        "srt",
        "The start revision timestamp"
    )
    val endRevisionTimestamp: LocalDateTime by option(
        CliArgType.Timestamp
        "end-revision-timestamp",
        "ert",
        "The end revision timestamp"
    )

    
    override fun createCliCommand(options: CliOptions): CliCommand {
        return QueryCommand(options)
    }


}