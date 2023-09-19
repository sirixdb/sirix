package io.sirix.rest.crud.xml

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import io.sirix.access.Databases
import io.sirix.access.trx.node.HashType
import io.sirix.api.Database
import io.sirix.api.xml.XmlNodeReadOnlyTrx
import io.sirix.api.xml.XmlResourceSession
import io.sirix.rest.crud.AbstractHeadHandler
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId

class XmlHead(private val location: Path): AbstractHeadHandler<XmlResourceSession>(location) {
    override fun openDatabase(dbFile: Path): Database<XmlResourceSession> {
        return Databases.openXmlDatabase(dbFile)
    }
}