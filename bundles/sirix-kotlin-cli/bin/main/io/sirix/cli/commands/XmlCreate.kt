package io.sirix.cli.commands

import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.service.xml.shredder.XmlShredder
import java.io.FileInputStream
import java.nio.file.Paths
import javax.xml.stream.XMLEventReader

class XmlCreate(options: io.sirix.cli.CliOptions, private val dataOptions: DataCommandOptions?) :
    AbstractCreate(options, dataOptions) {

    override fun createDatabase(): Boolean {
        return Databases.createXmlDatabase(
            DatabaseConfiguration(
                Paths.get(options.location)
            )
        )
    }

    override fun insertData() {
        val database = openXmlDatabase(dataOptions!!.user)

        createOrRemoveAndCreateResource(database)
        val manager = database.beginResourceSession(dataOptions.resourceName)
        manager.use {
            val wtx = manager.beginNodeTrx()
            wtx.use {
                wtx.insertSubtreeAsFirstChild(eventStream())
                if (dataOptions.commitMessage.isNotEmpty()) {
                    wtx.commit(dataOptions.commitMessage)
                }
            }
        }
    }

    private fun eventStream(): XMLEventReader {
        if (dataOptions!!.data.isNotEmpty()) {
            return XmlShredder.createStringReader(dataOptions.data)
        } else if (dataOptions.datafile.isNotEmpty()) {
            return XmlShredder.createFileReader(FileInputStream(dataOptions.datafile))
        }
        throw IllegalStateException("At least data or datafile has to be set!")
    }
}
