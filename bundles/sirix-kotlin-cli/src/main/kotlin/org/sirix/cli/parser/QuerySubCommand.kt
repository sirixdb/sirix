package org.sirix.cli.parser

import kotlinx.cli.ArgType
import kotlinx.cli.optional
import kotlinx.cli.required
import org.sirix.cli.CliOptions
import org.sirix.cli.MetaDataEnum
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.Query
import org.sirix.cli.commands.QueryOptions

class QuerySubCommand :
    AbstractUserCommand("query", "Querys the Database") {

    val resource by option(ArgType.String, "resource", "r", "The name of the resource.").required()
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
    val nodeId by option(CliArgType.Long(), "node-id", "nid", "The node id")
    val nextTopLevelNodes by option(
        ArgType.Int,
        "next-top-level-nodes",
        "ntln",
        "The next top level Node. Ignored for XML Databases"
    )
    val lastTopLevelNodeKey by option(
        CliArgType.Long(),
        "last-top-level-node-key",
        "ltlnk",
        "The last top level Node Key"
    )
    val maxLevel by option(CliArgType.Long(), "max-level", "ml", "The max Level")
    val metaData by option(
        ArgType.Choice(listOf("nodeKeyAndChildCount", "nodeKey", "all")),
        "meta-data",
        "md",
        "Print out meta data with the result. Ignored for XML Databases"
    )
    val prettyPrint by option(ArgType.Boolean, "pretty-print", "pp", "Print out formated result")
    val queryStr by argument(ArgType.String, description = "The Query String").optional()

    override fun createCliCommand(options: CliOptions): CliCommand {
        return Query(
            options,
            QueryOptions(
                queryStr,
                resource,
                revision,
                revisionTimestamp,
                startRevision,
                endRevision,
                startRevisionTimestamp,
                endRevisionTimestamp,
                nodeId,
                nextTopLevelNodes,
                lastTopLevelNodeKey,
                maxLevel,
                toMentaDataEnum(metaData),
                prettyPrint ?: false,
                user
            )
        )
    }

    private fun toMentaDataEnum(metaData: String?): MetaDataEnum {
        if (metaData == null) {
            return MetaDataEnum.NONE
        }
        return when (metaData) {
            "nodeKeyAndChildCount" -> MetaDataEnum.NODE_KEY_AND_CHILD_COUNT
            "nodeKey" -> MetaDataEnum.NODE_KEY
            "all" -> MetaDataEnum.ALL
            else -> throw IllegalArgumentException("Unkown Metadata Type $metaData")
        }
    }

}
