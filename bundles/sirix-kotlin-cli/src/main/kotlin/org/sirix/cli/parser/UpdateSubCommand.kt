package org.sirix.cli.parser

import kotlinx.cli.ArgType
import kotlinx.cli.required
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.Update

class UpdateSubCommand : AbstractUserCommand("update", "Update command") {

    val resource by option(ArgType.String, "resource", "r", "The name of the resource.").required()
    val nodeId by option(CliArgType.Long(), "node-id", "nid", "The node id.")
    val insertMode by option(
        ArgType.Choice(listOf("as-first-child", "as-right-sibling", "as-left-sibling", "replace")),
        "insert-mode",
        "im",
        "JSON Databases: Required. Valid vales: [as-first-child, as-right-sibling]. XML Databases: Mapped to 'replace' if not set"
    )
    val updateStr by argument(ArgType.String, description = "The Udate String.")

    override fun createCliCommand(options: CliOptions): CliCommand {
        return Update(options, updateStr, resource, insertMode, nodeId, user)
    }
}
