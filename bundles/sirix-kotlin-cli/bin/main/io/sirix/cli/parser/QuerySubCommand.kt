package io.sirix.cli.parser

import kotlinx.cli.ArgType
import kotlinx.cli.optional
import kotlinx.cli.required
import io.sirix.cli.MetaDataEnum
import io.sirix.cli.commands.CliCommand
import io.sirix.cli.commands.Query
import io.sirix.cli.commands.QueryOptions

class QuerySubCommand :
    AbstractUserCommand("query", "Querys the Database") {

    val resource by option(ArgType.String, "resource", "r", "The name of the resource.").required()
    private val revision by option(ArgType.Int, "revision", "rev", "The revions to query")
    private val revisionTimestamp by option(
        CliArgType.Timestamp(),
        "revision-timestamp",
        "rt",
        "The revision timestamp"
    )
    private val startRevision by option(ArgType.Int, "start-revision", "sr", "The start revision")
    private val endRevision by option(ArgType.Int, "end-revision", "er", "The end revision")
    private val startRevisionTimestamp by option(
        CliArgType.Timestamp(),
        "start-revision-timestamp",
        "srt",
        "The start revision timestamp"
    )
    private val endRevisionTimestamp by option(
        CliArgType.Timestamp(),
        "end-revision-timestamp",
        "ert",
        "The end revision timestamp"
    )
    private val nodeId by option(CliArgType.Long(), "node-id", "nid", "The node id")
    private val nextTopLevelNodes by option(
        ArgType.Int,
        "next-top-level-nodes",
        "ntln",
        "The next top level Node. Ignored for XML Databases"
    )
    private val lastTopLevelNodeKey by option(
        CliArgType.Long(),
        "last-top-level-node-key",
        "ltlnk",
        "The last top level Node Key"
    )
    private val startResultSeqIndex by option(
        CliArgType.Long(),
        "start-result-sequence-index",
        "srsi",
        "The start Result Sequence Index"
    )
    private val endResultSeqIndex by option(
        CliArgType.Long(),
        "end-result-sequence-index",
        "ersi",
        "The end Result Sequence Index"
    )
    private val maxLevel by option(CliArgType.Long(), "max-level", "ml", "The max Level")
    private val metaData by option(
        ArgType.Choice(listOf("nodeKeyAndChildCount", "nodeKey", "all")),
        "meta-data",
        "md",
        "Print out meta data with the result. Ignored for XML Databases"
    )
    private val prettyPrint by option(ArgType.Boolean, "pretty-print", "pp", "Print out formated result")
    private val queryStr by argument(ArgType.String, description = "The Query String").optional()

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
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
                startResultSeqIndex,
                endResultSeqIndex,
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
