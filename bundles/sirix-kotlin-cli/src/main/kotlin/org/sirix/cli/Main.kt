@file:UseExperimental(ExperimentalCli::class)

package org.sirix.cli


import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.ExperimentalCli
import kotlinx.cli.Subcommand
import kotlinx.cli.required
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.parser.AbstractArgSubCommand
import org.sirix.cli.parser.CreateResourceSubCommand
import org.sirix.cli.parser.CreateSubcommand
import org.sirix.cli.parser.DropResourceSubCommand
import org.sirix.cli.parser.DropSubCommand
import org.sirix.cli.parser.DumpResourceHistorySubCommand
import org.sirix.cli.parser.QuerySubCommand


fun main(args: Array<String>) {
    val cliCommand: CliCommand? = parseArgs(args)

    if (cliCommand != null) {
        cliCommand.execute()
    } else {
        println("No Command provided!")
    }
}


fun parseArgs(args: Array<String>): CliCommand? {
    val argParser = ArgParser("Sirix CLI")

    val location by argParser.option(ArgType.String, "location", "l", "The Sirix DB File location").required()
    // Can not use default values when using subcommands at the moment.
    // See https://github.com/Kotlin/kotlinx.cli/issues/26
    val verbose by argParser.option(ArgType.Boolean, "verbose", "v", "Run verbosely")

    val subCommandList: Array<Subcommand> = arrayOf(
        CreateSubcommand(),
        DropSubCommand(),
        CreateResourceSubCommand(),
        DropResourceSubCommand(),
        DumpResourceHistorySubCommand(),
        QuerySubCommand()
    )
    argParser.subcommands(*subCommandList)
    argParser.parse(args)

    val options = CliOptions(location, verbose ?: false)

    subCommandList.forEach {
        val asc: AbstractArgSubCommand = it as AbstractArgSubCommand
        if (asc.wasExecuted()) {
            return asc.createCliCommand(options)
        }
    }

    return null
}


