@file:OptIn(ExperimentalCli::class)
package io.sirix.cli.parser

import io.sirix.cli.commands.CliCommand
import kotlinx.cli.ExperimentalCli
import kotlinx.cli.Subcommand

abstract class AbstractArgSubCommand(name: String, actionDescription: String) : Subcommand(name, actionDescription) {

    private var executed: Boolean = false

    override fun execute() {
        executed = true
    }

    fun wasExecuted() : Boolean {
        return executed
    }

    abstract fun createCliCommand(options: io.sirix.cli.CliOptions) : CliCommand
}