package ord.sirix.cli

import kotlinx.cli.*

fun main(args: Array<String>) {
    // --file --verbose

    parseArds(args)

}


fun parseArds(args: Array<String>): ArgParser {
    val parser = ArgParser("Sirix CLI")

    val file by parser.option(ArgType.String, shortName = "f", fullName = "file", description = "Sirix DB File").required()
    val verbose by parser.option(ArgType.Boolean, shortName = "v", fullName = "verbose", description = "Print out verbose informations").default(false)

    parser.parse(args)

    val config =  Config(file)
    config.verbose = verbose

    return parser
}

