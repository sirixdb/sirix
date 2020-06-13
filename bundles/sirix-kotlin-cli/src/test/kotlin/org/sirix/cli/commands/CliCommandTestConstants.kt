package org.sirix.cli.commands

import org.sirix.access.User
import java.util.*

class CliCommandTestConstants {

    companion object {
        @JvmField
        val TEST_USER: User = User("testuser", UUID.fromString("091a12f2-e0dc-4795-abde-4b6b9c2932d1"))

        @JvmField
        val TEST_XML_DATA: String = "<xml><foo>Test</foo></xml>"

        @JvmField
        val TEST_JSON_DATA: String = "{\"json\":\"Test\"}"

        @JvmField
        val RESOURCE_LIST = listOf("resource1", "resource2", "resource3", "resource4", "resource5", "resource6")

        @JvmField
        val TEST_RESOURCE: String = "resource1"

        @JvmField
        val TEST_MESSAGE: String = "This is a test commit Message."
    }
}