package org.sirix.cli.commands

import com.google.gson.stream.JsonReader
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.api.json.JsonNodeTrx
import org.sirix.service.json.shredder.JsonShredder
import org.sirix.service.xml.shredder.XmlShredder
import org.slf4j.Logger
import java.io.File
import java.io.FileInputStream
import java.nio.file.Paths
import java.util.*
import javax.xml.stream.XMLEventReader

fun getTestFileCompletePath(fileName: String): String {
    return System.getProperty("java.io.tmpdir") + File.separator + fileName
}

fun removeTestDatabase(sirixTestFile: String, LOGGER: Logger) {
    LOGGER.info("Removing test Database\n$sirixTestFile")
    if (!File(sirixTestFile).deleteRecursively()) {
        LOGGER.error("Can not delete test Database!\n$sirixTestFile")
    }
}

fun createSirixTestFileName(): String {
    return getTestFileCompletePath("test-sirix-" + UUID.randomUUID() + ".db")
}

fun createJsonDatabase(sirixTestFile: String) {
    if (!Databases.createJsonDatabase(DatabaseConfiguration(Paths.get(sirixTestFile)))) {
        throw IllegalStateException("Can not create json Database!\n$sirixTestFile")
    }
}

fun createXmlDatabase(sirixTestFile: String) {
    if (!Databases.createXmlDatabase(DatabaseConfiguration(Paths.get(sirixTestFile)))) {
        throw IllegalStateException("Can not create xml Database!\n$sirixTestFile")
    }
}

fun setupTestDbJsonFromString(sirixQueryTestFileJson: String, testData: String) {
    setupTestDbJson(sirixQueryTestFileJson, JsonShredder.createStringReader(testData))
}


fun setupTestDbJsonFromFile(sirixQueryTestFileJson: String) {
    setupTestDbJson(
        sirixQueryTestFileJson,
        JsonShredder.createFileReader(Paths.get(CliCommandTestConstants.TEST_JSON_DATA_PATH))
    )
}

fun setupTestDbJson(sirixQueryTestFileJson: String, jsonReader: JsonReader) {
    val database =
        Databases.openJsonDatabase(Paths.get(sirixQueryTestFileJson), CliCommandTestConstants.TEST_USER)
    database.use {
        val resConfig = ResourceConfiguration.Builder(CliCommandTestConstants.TEST_RESOURCE).build()
        if (!database.createResource(resConfig)) {
            throw IllegalStateException("Failed to create resource '${CliCommandTestConstants.TEST_RESOURCE}'!")
        }
        val manager = database.openResourceManager(CliCommandTestConstants.TEST_RESOURCE)
        manager.use {
            val wtx = manager.beginNodeTrx()
            wtx.use {
                val insertSubtreeAsFirstChild =
                    wtx.insertSubtreeAsFirstChild(
                        jsonReader, JsonNodeTrx.Commit.No
                    )
                wtx.commit(CliCommandTestConstants.TEST_COMMIT_MESSAGE)
                val nodeKey = insertSubtreeAsFirstChild.node.nodeKey
                println("JSON Test Record nodeKey=$nodeKey")
            }
        }
    }
}


fun setupTestDbXmlFromString(sirixQueryTestFileXml: String, testData: String) {
    setupTestDbXml(sirixQueryTestFileXml, XmlShredder.createStringReader(testData))
}


fun setupTestDbXmlFromFile(sirixQueryTestFileXml: String) {
    setupTestDbXml(
        sirixQueryTestFileXml,
        XmlShredder.createFileReader(FileInputStream(CliCommandTestConstants.TEST_XML_DATA_PATH))
    )
}

fun setupTestDbXml(sirixQueryTestFileXml: String, xmlEventReader: XMLEventReader) {
    val database =
        Databases.openXmlDatabase(Paths.get(sirixQueryTestFileXml), CliCommandTestConstants.TEST_USER)
    database.use {
        val resConfig = ResourceConfiguration.Builder(CliCommandTestConstants.TEST_RESOURCE).build()
        if (!database.createResource(resConfig)) {
            throw IllegalStateException("Failed to create resource '${CliCommandTestConstants.TEST_RESOURCE}'!")
        }
        val manager = database.openResourceManager(CliCommandTestConstants.TEST_RESOURCE)
        manager.use {
            val wtx = manager.beginNodeTrx()
            wtx.use {
                val insertSubtreeAsFirstChild = wtx.insertSubtreeAsFirstChild(xmlEventReader)
                val nodeKey = insertSubtreeAsFirstChild.node.nodeKey
                println("XML Test Record nodeKey=$nodeKey")
                wtx.commit(CliCommandTestConstants.TEST_COMMIT_MESSAGE)
            }
        }
    }
}



