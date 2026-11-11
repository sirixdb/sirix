package io.sirix.rest.crud

import io.sirix.access.trx.node.AbstractResourceSession
import io.sirix.api.json.JsonResourceSession
import io.sirix.index.aggregate.TemporalAggregateIndexes
import io.sirix.index.aggregate.TemporalAggregateStorage
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import java.time.Duration

/**
 * Reads the temporal aggregate header from where the aggregate columns actually live.
 *
 * Aggregate columns are maintained in the hidden `<resource>$derived` resource, so reading the
 * source resource's own aggregate storage always answers `null` and compares equal to itself no
 * matter what a mutation did. This reads through the product API — [JsonResourceSession]'s
 * `openDerivedIndexReader` — so the bytes are the ones a query would serve.
 *
 * Maintenance is asynchronous, so every read first awaits the durable frontier and then *requires*
 * both the snapshot and the header to be present. A witness that quietly tolerated their absence
 * would decay into a null-against-null comparison, which is exactly the failure this replaces.
 */
internal object DerivedAggregateWitness {

    private val AWAIT: Duration = Duration.ofSeconds(30)

    /** The aggregate header bytes at [revision], never null. */
    fun header(session: JsonResourceSession, revision: Int = session.mostRecentRevisionNumber): ByteArray {
        val maintainer = (session as AbstractResourceSession<*, *>).derivedResource
        assertNotNull(maintainer, "Derived aggregate maintenance must be enabled, or there is no witness to read")
        assertTrue(
            maintainer!!.awaitRevision(revision, AWAIT),
            "Timed out awaiting derived aggregate maintenance of revision $revision"
        )
        session.beginNodeReadOnlyTrx(revision).use { reader ->
            val derived = session.openDerivedIndexReader(reader.storageEngineReader)
            assertNotNull(derived, "The derived aggregate snapshot must be readable at revision $revision")
            derived!!.use { snapshot ->
                TemporalAggregateStorage.Reader(snapshot, 0).use { aggregates ->
                    val header = aggregates.get(TemporalAggregateIndexes.headerKey())
                    assertNotNull(header, "The derived aggregate header must exist at revision $revision")
                    return header!!
                }
            }
        }
    }
}
