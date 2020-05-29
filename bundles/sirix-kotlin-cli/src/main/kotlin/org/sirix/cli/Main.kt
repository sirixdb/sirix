package org.sirix.cli

import kotlinx.cli.*

fun main(args: Array<String>) {
    // --file --verbose

    parseArgs(args)

}


fun parseArgs(args: Array<String>): Config {
    val parser = ArgParser("Sirix CLI")

    val file by parser.option(ArgType.String, shortName = "f", fullName = "file", description = "Sirix DB File").required()
    val verbose by parser.option(ArgType.Boolean, shortName = "v", fullName = "verbose", description = "Run verbosely").default(false)

    parser.parse(args)

    val config =  Config(file, verbose)

    return config
}

