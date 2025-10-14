package io.sirix.cli.parser

import kotlinx.cli.ArgType
import io.sirix.access.User


abstract class AbstractUserCommand(name: String, actionDescription: String) :
    AbstractArgSubCommand(name, actionDescription) {
    private val username by option(
        ArgType.String,
        "username",
        "un",
        "The user name who interacts with the Database. Default is 'admin'."
    )
    private val userId by option(CliArgType.Uuid, "userid", "uid", "The user UUID. Used when the user is set.")

    var user: User? = null

    override fun execute() {
        super.execute()
        if (username != null) {
            user = User(username, userId)
        }
    }
}