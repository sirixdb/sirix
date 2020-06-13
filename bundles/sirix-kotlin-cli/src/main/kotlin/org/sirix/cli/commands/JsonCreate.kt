package org.sirix.cli.commands

import com.google.gson.stream.JsonReader
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.cli.CliOptions
import org.sirix.service.json.shredder.JsonShredder
import java.nio.file.Paths

class JsonCreate(options: CliOptions, private val dataOptions: DataCommandOptions?): Create(options, dataOptions) {


    override fun createDatabase(): Boolean {
        return Databases.createJsonDatabase(DatabaseConfiguration(Paths.get(options.file)))
    }

    override fun insertData() {
        val database = openJsonDatabase(dataOptions!!.user)

        createOrRemoveAndCreateResource(database)
        val manager = database.openResourceManager(dataOptions!!.resourceName)
        manager.use {
            val wtx = manager.beginNodeTrx()
            wtx.use {
                wtx.insertSubtreeAsFirstChild(jsonReader())
                if (dataOptions.commitMessage != null) {
                    wtx.commit(dataOptions.commitMessage)
                }
            }
        }
    }


    private fun jsonReader(): JsonReader {
        if (dataOptions!!.data.isNotEmpty()) {
            return JsonShredder.createStringReader(dataOptions!!.data)
        } else if (dataOptions!!.datafile.isNotEmpty()) {
            return JsonShredder.createFileReader(Paths.get(dataOptions.datafile))
        }
        throw IllegalStateException("At least data or datafile has to be set!")
    }


}