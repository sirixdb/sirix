package org.sirix.cli.commands

import org.sirix.api.xml.XmlNodeTrx
import javax.xml.stream.XMLEventReader

enum class XmlInsertionMode {
    ASFIRSTCHILD {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsFirstChild(xmlReader)
        }
    },
    ASRIGHTSIBLING {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsRightSibling(xmlReader)
        }
    },
    ASLEFTSIBLING {
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
        fun getInsertionModeByName(name: String) = valueOf(name.toUpperCase())
    }
}
