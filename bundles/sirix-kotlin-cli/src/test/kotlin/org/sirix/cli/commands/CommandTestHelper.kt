package org.sirix.cli.commands

import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.slf4j.Logger
import java.io.File
import java.nio.file.Paths
import java.util.*


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
    return getTestFileCompletePath("test_sirix-" + UUID.randomUUID() + ".db")
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

