package org.sirix.cli.commands

import org.sirix.access.User
import java.util.*

class CliCommandTestConstants {

    companion object {
        @JvmField
        val TEST_USER: User = User("testuser", UUID.fromString("091a12f2-e0dc-4795-abde-4b6b9c2932d1"))

        @JvmField
        val TEST_XML_DATA: String =
            "<xml-data><bar><hello>world</hello><helloo>true</helloo></bar><baz>hello</baz><foo><element>bar</element><element null=\"true\"/><element>2.33</element></foo><tada><element><foo>bar</foo></element><element><baz>false</baz></element><element>boo</element><element/><element/></tada></xml-data>"

        @JvmField
        val TEST_XML_DATA_MODIFIED: String =
            "<xml-data><bar><helloo>world</helloo><hello>true</hello></bar><baz>hello</baz><foo><element>bar</element><element null=\"true\"/><element>2.33</element></foo><tatrata-tada><element><foo>bar</foo></element><element><baz>false</baz></element><element>boo</element><element/><element/></tatrata-tada></xml-data>"


        @JvmField
        val TEST_XML_DATA_PATH: String = {}::class.java.getResource("/org/sirix/cli/commands/test_data.xml").path

        @JvmField
        val TEST_JSON_DATA: String =
            "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}"

        @JvmField
        val TEST_JSON_DATA_MODIFIED: String =
            "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"helloo\":\"world\",\"hello\":true},\"baz\":\"hello\",\"tadara tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}"

        @JvmField
        val TEST_JSON_DATA_PATH: String = {}::class.java.getResource("/org/sirix/cli/commands/test_data.json").path

        @JvmField
        val RESOURCE_LIST = listOf("resource1", "resource2", "resource3", "resource4", "resource5", "resource6")

        @JvmField
        val TEST_RESOURCE: String = "resource1"

        @JvmField
        val TEST_COMMIT_MESSAGE: String = "This is a test commit Message."
    }
}
