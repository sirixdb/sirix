package io.sirix.cli.parser

import kotlinx.cli.ArgType
import kotlinx.cli.required
import io.sirix.cli.commands.CliCommand
import io.sirix.cli.commands.Update

class UpdateSubCommand : AbstractUserCommand("update", "Update command") {

    val resource by option(ArgType.String, "resource", "r", "The name of the resource.").required()
    private val nodeId by option(CliArgType.Long(), "node-id", "nid", "The node id.")
    private val insertMode by option(
        ArgType.Choice(listOf("as-first-child", "as-right-sibling", "as-left-sibling", "replace")),
        "insert-mode",
        "im",
        "JSON Databases: Required. Valid vales: [as-first-child, as-right-sibling]. XML Databases: Mapped to 'replace' if not set"
    )
    private val updateStr by argument(ArgType.String, description = "The Udate String.")

    override fun createCliCommand(options: io.sirix.cli.CliOptions): CliCommand {
        return Update(options, updateStr, resource, insertMode, nodeId, user)
    }
}
