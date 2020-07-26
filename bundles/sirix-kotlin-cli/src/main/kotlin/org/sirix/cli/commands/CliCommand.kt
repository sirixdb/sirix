package org.sirix.cli.commands

import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.access.Databases.openJsonDatabase
import org.sirix.access.Databases.openXmlDatabase
import org.sirix.access.User
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.api.xml.XmlResourceManager
import org.sirix.cli.CliOptions
import org.sirix.cli.CliPrinter
import java.nio.file.Path
import java.nio.file.Paths

abstract class CliCommand(protected val options: CliOptions) {
    protected val cliPrinter = CliPrinter(options.verbose)

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

    protected fun openJsonDatabase(user: User?): Database<JsonResourceManager> {
        return when {
            user != null -> {
                openJsonDatabase(path(), user)
            }
            else -> {
                openJsonDatabase(path())
            }
        }
    }

    protected fun openXmlDatabase(user: User?): Database<XmlResourceManager> {
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
