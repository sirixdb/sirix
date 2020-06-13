@file:UseExperimental(ExperimentalCli::class)
package org.sirix.cli.parser

import kotlinx.cli.ExperimentalCli
import kotlinx.cli.Subcommand
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand

abstract class AbstractArgSubCommand(name: String, actionDescription: String) : Subcommand(name, actionDescription) {

    protected var executed: Boolean = false

    override fun execute() {
        executed = true
    }

    fun wasExecuted() : Boolean {
        return executed
    }

    abstract fun createCliCommand(options: CliOptions) : CliCommand
}