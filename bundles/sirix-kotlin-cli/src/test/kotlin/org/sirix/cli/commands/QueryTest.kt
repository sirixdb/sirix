package org.sirix.cli.commands

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.cli.CliOptions
import org.sirix.cli.MetaDataEnum
import org.sirix.service.json.shredder.JsonShredder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import java.util.*

internal class QueryTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(QueryTest::class.java)

        @JvmField
        val sirixQueryTestFile = getTestFileCompletePath("test_sirix-" + UUID.randomUUID() + ".db")


        @BeforeAll
        @JvmStatic
        internal fun setup() {
            createJsonDatabase(sirixQueryTestFile)
            setupTestDb()
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            removeTestDatabase(
                sirixQueryTestFile,
                LOGGER
            )
        }

    }

    private fun queryOptions() = listOf<QueryOptions>(
        QueryOptions(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            MetaDataEnum.NONE,
            false,
            null
        )
    )


    @ParameterizedTest
    @MethodSource("queryOptions")
    fun happyPath(queryOptions: QueryOptions) {
        Query(CliOptions(sirixQueryTestFile, true), queryOptions).execute()
    }
}


private fun setupTestDb() {
    val database =
        Databases.openJsonDatabase(Paths.get(QueryTest.sirixQueryTestFile), CliCommandTestConstants.TEST_USER)
    database.use {
        val resConfig = ResourceConfiguration.Builder(CliCommandTestConstants.TEST_RESOURCE).build()
        if (!database.createResource(resConfig)) {
            throw IllegalStateException("Failed to create resource '${CliCommandTestConstants.TEST_RESOURCE}'!")
        }
        val manager = database.openResourceManager(CliCommandTestConstants.TEST_RESOURCE)
        manager.use {
            val wtx = manager.beginNodeTrx()
            wtx.use {
                wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(Paths.get(CliCommandTestConstants.TEST_JSON_DATA_PATH)))
                wtx.commit(CliCommandTestConstants.TEST_COMMIT_MESSAGE)
            }
        }
    }
}



