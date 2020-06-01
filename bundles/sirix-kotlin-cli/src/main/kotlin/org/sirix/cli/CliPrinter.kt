package org.sirix.cli

class CliPrinter(private val verbose: Boolean) {

    fun prnLn(message: String) {
        println(message)
    }

    fun prnLnV(message: String) {
        if (verbose) {
            prnLn(message)
        }
    }

}