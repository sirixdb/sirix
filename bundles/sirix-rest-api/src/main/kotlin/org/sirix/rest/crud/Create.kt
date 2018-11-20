package org.sirix.rest.crud

import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext
import org.sirix.access.Databases
import org.sirix.access.conf.DatabaseConfiguration
import org.sirix.access.conf.ResourceConfiguration
import org.sirix.rest.Serialize
import org.sirix.service.xml.serialize.XMLSerializer
import org.sirix.service.xml.shredder.XMLShredder
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path

// For instance: curl -k -X POST -d "<xml/>" -u admin https://localhost:8443/database/resource1
class Create(private val location: Path) : Handler<RoutingContext> {
    override fun handle(ctx: RoutingContext) {
        val database = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")
        val resToStore = ctx.bodyAsString

        if (database == null || resToStore == null)
            ctx.fail(IllegalArgumentException("Database name and resource data to store not given."))

        shredder(database, resource, resToStore, ctx)
    }

    private fun shredder(dbPathName: String, resPathName: String = dbPathName, resFileToStore: String, ctx: RoutingContext) {
        val dbFile = location.resolve(dbPathName)

        val dbExists = Files.exists(dbFile)

        if (!dbExists)
            Files.createDirectories(dbFile.parent)

        val dbConfig = DatabaseConfiguration(dbFile)

        if (!Databases.existsDatabase(dbFile)) {
            Databases.createDatabase(dbConfig)
        }

        val database = Databases.openDatabase(dbFile)

        database.use {
            val resConfig = ResourceConfiguration.Builder(resPathName, dbConfig).build()

            if (!database.createResource(resConfig)) {
                database.removeResource(resPathName)
                database.createResource(resConfig)
            }

            val manager = database.getResourceManager(resPathName)

            manager.use {
                val wtx = manager.beginNodeWriteTrx()

                wtx.use {
                    wtx.insertSubtreeAsFirstChild(XMLShredder.createStringReader(resFileToStore))
                }

                val out = ByteArrayOutputStream()
                val serializerBuilder = XMLSerializer.XMLSerializerBuilder(manager, out)
                val serializer = serializerBuilder.emitIDs().emitRESTful().prettyPrint().build()

                Serialize().serializeXml(serializer, out, ctx)
            }
        }
    }
}