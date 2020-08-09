package org.sirix.cli.commands

import org.sirix.access.DatabaseType
import org.sirix.access.User
import org.sirix.access.trx.node.HashType
import org.sirix.api.xml.XmlNodeTrx
import org.sirix.cli.CliOptions
import org.sirix.service.xml.shredder.XmlShredder
import java.math.BigInteger
import javax.xml.stream.XMLEventReader

class Update(
    options: CliOptions,
    val updateStr: String,
    val resource: String,
    val insertMode: String?,
    val nodeId: Long?,
    val hashCode: BigInteger?,
    val user: User?
) : CliCommand(options) {

    override fun execute() {
        val startMillis: Long = System.currentTimeMillis()
        cliPrinter.prnLnV("Execute Update")
        val result: String? = when (databaseType()) {
            DatabaseType.XML -> updateXml()
            DatabaseType.JSON -> updateJson()
            else -> throw IllegalArgumentException("Unknown Database Type!")
        }
        if (result != null) {
            cliPrinter.prnLn(result)
        }
        cliPrinter.prnLnV("Query executed (${System.currentTimeMillis() - startMillis}ms)")
    }

    private fun updateJson(): String? {
        
        return null
    }

    private fun updateXml(): String? {

        val database = openXmlDatabase(user)

        database.use {
            val manager = database.openResourceManager(resource)

            manager.use {
                val wtx = manager.beginNodeTrx()

                manager.use {
                    val wtx = manager.beginNodeTrx()
                    val (maxNodeKey, hash) = wtx.use {
                        if (nodeId != null) {
                            wtx.moveTo(nodeId)
                        }

                        if (wtx.isDocumentRoot && wtx.hasFirstChild())
                            wtx.moveToFirstChild()

                        if (manager.resourceConfig.hashType != HashType.NONE && !wtx.isDocumentRoot) {
                            if (hashCode == null) {
                                IllegalStateException("Hash code is missing required.")
                            }

                            if (wtx.hash != hashCode) {
                                IllegalArgumentException("Someone might have changed the resource in the meantime.")
                            }
                        }

                        val xmlReader = XmlShredder.createStringReader(resource)
                        if (insertMode != null)
                            XmlInsertionMode.getInsertionModeByName(insertMode).insert(wtx, xmlReader)
                        else
                            wtx.replaceNode(xmlReader)

                        if (nodeId != null)
                            wtx.moveTo(nodeId)

                        if (wtx.isDocumentRoot && wtx.hasFirstChild())
                            wtx.moveToFirstChild()

                        Pair(wtx.maxNodeKey, wtx.hash)
                    }
                    if (maxNodeKey > 5000) {
                        if (manager.resourceConfig.hashType == HashType.NONE) {
                            return null
                        } else {
                            return hash.toString()
                        }
                    } else {
                        return SerializerAdapter(manager, null).prettyPrint(true).startNodeKey(nodeId).serialize()
                    }
                }
            }
        }
    }
}

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

