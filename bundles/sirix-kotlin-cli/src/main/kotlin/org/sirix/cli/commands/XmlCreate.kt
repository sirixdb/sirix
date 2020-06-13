package org.sirix.cli.commands

import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.cli.CliOptions
import org.sirix.service.xml.shredder.XmlShredder
import java.io.FileInputStream
import java.nio.file.Paths
import javax.xml.stream.XMLEventReader

class XmlCreate(options: CliOptions, private val dataOptions: DataCommandOptions?): Create(options, dataOptions) {


    override fun createDatabase(): Boolean {
        return Databases.createXmlDatabase(DatabaseConfiguration(Paths.get(options.file)))
    }

    override fun insertData() {
        val database = openXmlDatabase(dataOptions!!.user)

        createOrRemoveAndCreateResource(database)
        val manager = database.openResourceManager(dataOptions!!.resourceName)
        manager.use {
            val wtx = manager.beginNodeTrx()
            wtx.use {
                wtx.insertSubtreeAsFirstChild(eventStream())
                if (dataOptions.commitMessage != null) {
                    wtx.commit(dataOptions.commitMessage)
                }
            }
        }
    }


    private fun eventStream(): XMLEventReader {
        if (dataOptions!!.data.isNotEmpty()) {
            return XmlShredder.createStringReader(dataOptions!!.data)
        } else if (dataOptions!!.datafile.isNotEmpty()) {
            return XmlShredder.createFileReader(FileInputStream(dataOptions.datafile))
        }
        throw IllegalStateException("At least data or datafile has to be set!")
    }


}