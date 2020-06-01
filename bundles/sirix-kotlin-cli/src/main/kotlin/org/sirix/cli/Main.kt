@file:UseExperimental(ExperimentalCli::class)
package org.sirix.cli


import com.google.inject.internal.cglib.core.`$ClassInfo`
import kotlinx.cli.*
import org.sirix.access.DatabaseType
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.Create


fun main(args: Array<String>) {
    val cliCommand: CliCommand?  = parseArgs(args)

    if(cliCommand != null) {
        cliCommand.execute()
    } else {
        println("No Command provided!")
    }
}

fun parseArgs1(args: Array<String>): CliCommand? {
    val argParser = ArgParser("Sirix CLI")

    val verbose by argParser.option(ArgType.Boolean,"verbose", "v" ,  "Run verbosely").default(value = false)
    val file by argParser.option(ArgType.String, "file", "f",  "Sirix DB File").required()

    argParser.parse(args)
    val options = CliOptions(file, verbose)

    return null
}


fun parseArgs(args: Array<String>): CliCommand? {
    val argParser = ArgParser("Sirix CLI")

    val file by argParser.option(ArgType.String, "file",  "f" ,  "Sirix DB File").required()
    // Can not use default values when using subcommands at the moment.
    // See https://github.com/Kotlin/kotlinx.cli/issues/26
    val verbose by argParser.option(ArgType.Boolean,"verbose",  "v",   "Run verbosely")

    // TODO Externalize class
    class CreateSubcommand : ArgSubCommand("create", "Create a Sirix DB") {
        val type by argument(ArgType.Choice(listOf("xml", "json")), "The Type of the Database")
        val data by option(ArgType.String, "data ", "d", "Data to insert into the Database")


        var dataBasetype: DatabaseType? = null

        override fun execute() {
            dataBasetype = DatabaseType.valueOf(type.toUpperCase())
        }

        override fun isValid(): Boolean {
            return dataBasetype != null
        }

        override fun createCliCommand(options: CliOptions) : CliCommand {
                return Create(options, dataBasetype!!)
        }
    }

    val subCommandList: Array<Subcommand> = arrayOf(CreateSubcommand())
    argParser.subcommands(*subCommandList)
    argParser.parse(args)

    val options = CliOptions(file, verbose ?: false)

    subCommandList.forEach {
        val asc: ArgSubCommand  = it as ArgSubCommand
        if (asc.isValid()) {
            return asc.createCliCommand(options)
        }
    }


    return null
}


abstract class ArgSubCommand(name: String, actionDescription: String) : Subcommand(name, actionDescription) {

    abstract fun createCliCommand(options: CliOptions) : CliCommand

    abstract fun isValid() : Boolean

}