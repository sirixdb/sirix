@file:UseExperimental(ExperimentalCli::class)
package org.sirix.cli


import kotlinx.cli.*
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.parser.CreateSubcommand
import org.sirix.cli.parser.AbstractArgSubCommand


fun main(args: Array<String>) {
    val cliCommand: CliCommand?  = parseArgs(args)

    if(cliCommand != null) {
        cliCommand.execute()
    } else {
        println("No Command provided!")
    }
}


fun parseArgs(args: Array<String>): CliCommand? {
    val argParser = ArgParser("Sirix CLI")

    val file by argParser.option(ArgType.String, "file",  "f" ,  "Sirix DB File").required()
    // Can not use default values when using subcommands at the moment.
    // See https://github.com/Kotlin/kotlinx.cli/issues/26
    val verbose by argParser.option(ArgType.Boolean,"verbose",  "v",   "Run verbosely")

    val subCommandList: Array<Subcommand> = arrayOf(CreateSubcommand())
    argParser.subcommands(*subCommandList)
    argParser.parse(args)

    val options = CliOptions(file, verbose ?: false)

    subCommandList.forEach {
        val asc: AbstractArgSubCommand  = it as AbstractArgSubCommand
        if (asc.isValid()) {
            return asc.createCliCommand(options)
        }
    }


    return null
}


