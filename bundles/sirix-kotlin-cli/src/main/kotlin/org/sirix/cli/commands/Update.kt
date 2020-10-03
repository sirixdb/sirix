package org.sirix.cli.commands

import com.google.gson.stream.JsonToken
import org.sirix.access.DatabaseType
import org.sirix.access.User
import org.sirix.access.trx.node.HashType
import org.sirix.cli.CliOptions
import org.sirix.service.json.shredder.JsonShredder
import org.sirix.service.xml.shredder.XmlShredder
import java.math.BigInteger

class Update(
    options: CliOptions,
    private val updateStr: String,
    val resource: String,
    private val insertMode: String?,
    private val nodeId: Long?,
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
        val database = openJsonDatabase(user)

        database.use {
            val manager = database.openResourceManager(resource)

            manager.use {
                val wtx = manager.beginNodeTrx()
                val (maxNodeKey, hash) = wtx.use {
                    if (nodeId != null) {
                        wtx.moveTo(nodeId)
                    }

                    if (wtx.isDocumentRoot && wtx.hasFirstChild()) {
                        wtx.moveToFirstChild()
                    }

                    if (insertMode == null) {
                        throw IllegalArgumentException("Insertion mode must be given for JSON Databases.")
                    }

                    val jsonReader = JsonShredder.createStringReader(updateStr)
                    val insertionModeByName = JsonInsertionMode.getInsertionModeByName(insertMode)

                    if (jsonReader.peek() != JsonToken.BEGIN_ARRAY && jsonReader.peek() != JsonToken.BEGIN_OBJECT) {
                        when (jsonReader.peek()) {
                            JsonToken.STRING -> insertionModeByName.insertString(wtx, jsonReader)
                            JsonToken.NULL -> insertionModeByName.insertNull(wtx, jsonReader)
                            JsonToken.NUMBER -> insertionModeByName.insertNumber(wtx, jsonReader)
                            JsonToken.BOOLEAN -> insertionModeByName.insertBoolean(wtx, jsonReader)
                            JsonToken.NAME -> insertionModeByName.insertObjectRecord(wtx, jsonReader)
                            else -> throw IllegalStateException()
                        }
                    } else {
                        insertionModeByName.insertSubtree(wtx, jsonReader)
                    }

                    if (nodeId != null) {
                        wtx.moveTo(nodeId)
                    }

                    if (wtx.isDocumentRoot && wtx.hasFirstChild()) {
                        wtx.moveToFirstChild()
                    }

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

    private fun updateXml(): String? {
        val database = openXmlDatabase(user)

        database.use {
            val manager = database.openResourceManager(resource)

            manager.use {
                val wtx = manager.beginNodeTrx()
                val (maxNodeKey, hash) = wtx.use {
                    if (nodeId != null) {
                        wtx.moveTo(nodeId)
                    }

                    if (wtx.isDocumentRoot && wtx.hasFirstChild())
                        wtx.moveToFirstChild()

                    val xmlReader = XmlShredder.createStringReader(updateStr)

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
