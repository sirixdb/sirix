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

internal class QueryTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(QueryTest::class.java)

        @JvmField
        val sirixQueryTestFileJson = createSirixTestFileName()


        @BeforeAll
        @JvmStatic
        internal fun setup() {
            createJsonDatabase(sirixQueryTestFileJson)
            setupTestDbJson()
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            removeTestDatabase(
                sirixQueryTestFileJson,
                LOGGER
            )
        }

    }

    private fun queryOptionList() = listOf<QueryOptions>(
        QueryOptions(
            null,
            CliCommandTestConstants.TEST_RESOURCE,
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
            true,
            null
        )
    )


    @ParameterizedTest
    @MethodSource("queryOptionList")
    fun happyPath(queryOptions: QueryOptions) {
        Query(CliOptions(sirixQueryTestFileJson, true), queryOptions).execute()
    }
}


private fun setupTestDbJson() {
    val database =
        Databases.openJsonDatabase(Paths.get(QueryTest.sirixQueryTestFileJson), CliCommandTestConstants.TEST_USER)
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
