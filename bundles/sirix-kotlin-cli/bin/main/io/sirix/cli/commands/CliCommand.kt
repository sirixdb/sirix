package io.sirix.cli.commands

import io.sirix.access.DatabaseType
import io.sirix.access.Databases
import io.sirix.access.Databases.openJsonDatabase
import io.sirix.access.Databases.openXmlDatabase
import io.sirix.access.User
import io.sirix.api.Database
import io.sirix.api.json.JsonResourceSession
import io.sirix.api.xml.XmlResourceSession
import java.nio.file.Path
import java.nio.file.Paths

abstract class CliCommand(protected val options: io.sirix.cli.CliOptions) {
    protected val cliPrinter = io.sirix.cli.CliPrinter(options.verbose)

    abstract fun execute()

    protected fun path(): Path {
        return Paths.get(options.location)
    }

    protected fun databaseType(): DatabaseType = Databases.getDatabaseType(path())

    protected fun openDatabase(user: User?): Database<*> {
        return when (databaseType()) {
            DatabaseType.XML -> openXmlDatabase(user)
            DatabaseType.JSON -> openJsonDatabase(user)
            else -> throw IllegalStateException("Unknown Database Type!")
        }

    }

    protected fun openJsonDatabase(user: User?): Database<JsonResourceSession> {
        return when {
            user != null -> {
                openJsonDatabase(path(), user)
            }
            else -> {
                openJsonDatabase(path())
            }
        }
    }

    protected fun openXmlDatabase(user: User?): Database<XmlResourceSession> {
        return when {
            user != null -> {
                openXmlDatabase(path(), user)
            }
            else -> {
                openXmlDatabase(path())
            }
        }
    }
}
