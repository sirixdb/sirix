package io.sirix.cli.commands

import io.sirix.api.xml.XmlNodeTrx
import java.util.*
import javax.xml.stream.XMLEventReader

enum class XmlInsertionMode {
    AS_FIRST_CHILD {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsFirstChild(xmlReader)
        }
    },
    AS_RIGHT_SIBLING {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsRightSibling(xmlReader)
        }
    },
    AS_LEFT_SIBLING {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsLeftSibling(xmlReader)
        }
    },
    REPLACE {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.replaceNode(xmlReader)
        }
    };

    abstract fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader)

    companion object {
        fun getInsertionModeByName(name: String) = valueOf(name.replace('-', '_').uppercase(Locale.getDefault()))
    }
}
