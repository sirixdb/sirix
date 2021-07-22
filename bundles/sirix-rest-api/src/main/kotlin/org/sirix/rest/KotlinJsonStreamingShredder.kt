package org.sirix.rest

import com.google.common.base.Preconditions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.parsetools.JsonEventType
import io.vertx.core.parsetools.JsonParser
import org.sirix.access.trx.node.json.objectvalue.*
import org.sirix.api.json.JsonNodeTrx
import org.sirix.node.NodeKind
import org.sirix.service.InsertPosition
import org.sirix.settings.Fixed
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch

class KotlinJsonStreamingShredder(
    val wtx: JsonNodeTrx,
    val parser: JsonParser,
    var insert: InsertPosition = InsertPosition.AS_FIRST_CHILD,
    val latch: CountDownLatch
) : Callable<Long> {
    private val parents = ArrayDeque<Long>()

    private var level = 0

    override fun call(): Long {
        parents.add(Fixed.NULL_NODE_KEY.standardProperty)
        val revision: Long = wtx.revisionNumber.toLong()
        insertNewContent()
        return revision

    }

    private fun insertNewContent() {
        level = 0
        val insertedRootNodeKey = longArrayOf(-1)
        var keyValue: KeyValue? = null
        var lastEndObjectOrEndArrayEventType: JsonEventType? = null
        val parentKind = ArrayDeque<JsonEventType>()

        // Iterate over all nodes.
        parser.handler { event ->
            when (event.type()) {
                JsonEventType.START_OBJECT -> {
                    level++

                    if (keyValue != null) {
                        val value = keyValue!!.value
                        addObjectRecord(keyValue!!.field, value, value !is JsonObject && value !is JsonArray)
                        keyValue = null
                    }
                    if (lastEndObjectOrEndArrayEventType != null) {
                        processEndArrayOrEndObject(false)
                        lastEndObjectOrEndArrayEventType = null
                    }

                    val name = event.fieldName()
                    if (name == null || parentKind.first() != JsonEventType.START_OBJECT) {
                        val insertedObjectNodeKey = addObject()

                        if (insertedRootNodeKey[0] == -1L) {
                            insertedRootNodeKey[0] = insertedObjectNodeKey
                        }
                    } else {
                        keyValue = KeyValue(name, JsonObject())
                    }
                    parentKind.addFirst(event.type())
                }
                JsonEventType.VALUE -> {
                    val name = event.fieldName()
                    if (keyValue != null) {
                        val isNextTokenParentToken = name != null
                        addObjectRecord(keyValue!!.field, keyValue!!.value, isNextTokenParentToken)
                        keyValue = null
                    }
                    if (lastEndObjectOrEndArrayEventType != null) {
                        processEndArrayOrEndObject(name != null)
                        lastEndObjectOrEndArrayEventType = null
                    }
                    val value = event.value()
                    if (name != null) {
                        if (value == null) {
                            keyValue = KeyValue(name, NullValue())
                        } else {
                            keyValue = KeyValue(name, value)
                        }
                    } else if (value is Number) {
                        val insertedNumberValueNodeKey = insertNumberValue(value)

                        if (insertedRootNodeKey[0] == -1L)
                            insertedRootNodeKey[0] = insertedNumberValueNodeKey
                    } else if (value is String) {
                        val insertedStringValueNodeKey = insertStringValue(value)

                        if (insertedRootNodeKey[0] == -1L)
                            insertedRootNodeKey[0] = insertedStringValueNodeKey
                    } else if (value is Boolean) {
                        val insertedBooleanValueNodeKey = insertBooleanValue(value)

                        if (insertedRootNodeKey[0] == -1L)
                            insertedRootNodeKey[0] = insertedBooleanValueNodeKey
                    } else if (value == null) {
                        val insertedNullValueNodeKey = insertNullValue()

                        if (insertedRootNodeKey[0] == -1L)
                            insertedRootNodeKey[0] = insertedNullValueNodeKey
                    }
                }
                JsonEventType.END_OBJECT, JsonEventType.END_ARRAY -> {
                    if (keyValue != null) {
                        addObjectRecord(keyValue!!.field, keyValue!!.value, event.type() == JsonEventType.END_OBJECT)
                        keyValue = null
                    }
                    level--
                    if (lastEndObjectOrEndArrayEventType != null) {
                        processEndArrayOrEndObject(event.type() == JsonEventType.END_OBJECT)
                    }
                    lastEndObjectOrEndArrayEventType = event.type()
                    parentKind.removeFirst()
                }
                JsonEventType.START_ARRAY -> {
                    if (keyValue != null) {
                        val value = keyValue!!.value
                        addObjectRecord(keyValue!!.field, keyValue!!.value, value !is JsonObject && value !is JsonArray)
                        keyValue = null
                    }

                    if (lastEndObjectOrEndArrayEventType != null) {
                        processEndArrayOrEndObject(false)
                        lastEndObjectOrEndArrayEventType = null
                    }

                    level++

                    val name = event.fieldName()
                    if (name == null || parentKind.first() != JsonEventType.START_OBJECT) {
                        val insertedArrayNodeKey = insertArray()

                        if (insertedRootNodeKey[0] == -1L) {
                            insertedRootNodeKey[0] = insertedArrayNodeKey
                        }
                    } else {
                        keyValue = KeyValue(name, JsonArray())
                    }
                    parentKind.addFirst(event.type())
                }
            }
        }

        parser.endHandler {
            processEndArrayOrEndObject(false)
            wtx.moveTo(insertedRootNodeKey[0])
            latch.countDown()
        }
    }

    private fun processEndArrayOrEndObject(isNextTokenParentToken: Boolean) {
        parents.removeFirst()
        wtx.moveTo(parents.first())
        if (isNextTokenParentToken) {
            parents.removeFirst()
            wtx.moveTo(parents.first())
        }
    }

    private fun insertStringValue(stringValue: String): Long {
        val value = stringValue
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                    wtx.insertStringValueAsFirstChild(value).nodeKey
                } else {
                    wtx.insertStringValueAsRightSibling(value).nodeKey
                }
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                    wtx.insertStringValueAsLastChild(value).nodeKey
                } else {
                    wtx.insertStringValueAsRightSibling(value).nodeKey
                }
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertStringValueAsLeftSibling(value).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertStringValueAsRightSibling(value).nodeKey
        }

        adaptTrxPosAndStack(false, key)

        return key
    }

    private fun insertBooleanValue(boolValue: Boolean): Long {
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                    wtx.insertBooleanValueAsFirstChild(boolValue).nodeKey
                } else {
                    wtx.insertBooleanValueAsRightSibling(boolValue).nodeKey
                }
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (parents.last() == Fixed.NULL_NODE_KEY.standardProperty) {
                    wtx.insertBooleanValueAsLastChild(boolValue).nodeKey
                } else {
                    wtx.insertBooleanValueAsRightSibling(boolValue).nodeKey
                }
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertBooleanValueAsLeftSibling(boolValue).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertBooleanValueAsRightSibling(boolValue).nodeKey
        }

        adaptTrxPosAndStack(false, key)

        return key
    }

    private fun insertNumberValue(numberValue: Number): Long {
        val value = Preconditions.checkNotNull(numberValue)
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertNumberValueAsFirstChild(value).nodeKey
            } else {
                wtx.insertNumberValueAsRightSibling(value).nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertNumberValueAsLastChild(value).nodeKey
            } else {
                wtx.insertNumberValueAsRightSibling(value).nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertNumberValueAsLeftSibling(value).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertNumberValueAsRightSibling(value).nodeKey
            else -> throw AssertionError() //Should not happen
        }

        adaptTrxPosAndStack(false, key)
        return key
    }

    fun adaptTrxPosAndStack(nextTokenIsParent: Boolean, key: Long) {
        parents.removeFirst()

        if (nextTokenIsParent)
            wtx.moveTo(parents.first())
        else
            parents.addFirst(key)
    }

    private fun insertNullValue(): Long {
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertNullValueAsFirstChild().nodeKey
            } else {
                wtx.insertNullValueAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertNullValueAsLastChild().nodeKey
            } else {
                wtx.insertNullValueAsRightSibling().nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertNullValueAsLeftSibling().nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertNullValueAsRightSibling().nodeKey
            else -> throw AssertionError() //Should not happen
        }
        adaptTrxPosAndStack(false, key)
        return key
    }

    private fun insertArray(): Long {
        val key: Long
        when (insert) {
            InsertPosition.AS_FIRST_CHILD -> key = if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertArrayAsFirstChild().nodeKey
            } else {
                wtx.insertArrayAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> key = if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertArrayAsLastChild().nodeKey
            } else {
                wtx.insertArrayAsRightSibling().nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> {
                check(
                    !(wtx.kind === NodeKind.JSON_DOCUMENT
                            || wtx.parentKey == Fixed.DOCUMENT_NODE_KEY.standardProperty)
                ) { "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!" }
                key = wtx.insertArrayAsLeftSibling().nodeKey
                insert = InsertPosition.AS_FIRST_CHILD
            }
            InsertPosition.AS_RIGHT_SIBLING -> {
                check(
                    !(wtx.kind === NodeKind.JSON_DOCUMENT
                            || wtx.parentKey == Fixed.DOCUMENT_NODE_KEY.standardProperty)
                ) { "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!" }
                key = wtx.insertArrayAsRightSibling().nodeKey
                insert = InsertPosition.AS_FIRST_CHILD
            }
            else -> throw AssertionError() // Must not happen.
        }
        parents.removeFirst()
        parents.addFirst(key)
        parents.addFirst(Fixed.NULL_NODE_KEY.standardProperty)
        return key
    }

    private fun addObject(): Long {
        val key: Long
        when (insert) {
            InsertPosition.AS_FIRST_CHILD -> key = if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertObjectAsFirstChild().nodeKey
            } else {
                wtx.insertObjectAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> key = if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertObjectAsLastChild().nodeKey
            } else {
                wtx.insertObjectAsRightSibling().nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> {
                check(
                    !(wtx.kind === NodeKind.JSON_DOCUMENT
                            || wtx.parentKey == Fixed.DOCUMENT_NODE_KEY.standardProperty)
                ) { "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!" }
                key = wtx.insertObjectAsLeftSibling().nodeKey
                insert = InsertPosition.AS_FIRST_CHILD
            }
            InsertPosition.AS_RIGHT_SIBLING -> {
                check(
                    !(wtx.kind === NodeKind.JSON_DOCUMENT
                            || wtx.parentKey == Fixed.DOCUMENT_NODE_KEY.standardProperty)
                ) { "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!" }
                key = wtx.insertObjectAsRightSibling().nodeKey
                insert = InsertPosition.AS_FIRST_CHILD
            }
            else -> throw AssertionError() // Must not happen.
        }
        parents.removeFirst()
        parents.addFirst(key)
        parents.addFirst(Fixed.NULL_NODE_KEY.standardProperty)
        return key
    }

    private fun addObjectRecord(name: String?, objectValue: Any, isNextTokenParentToken: Boolean) {
        assert(name != null)
        val value: ObjectRecordValue<*> = getObjectRecordValue(objectValue)
        val key: Long = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertObjectRecordAsFirstChild(name, value).nodeKey
            } else {
                wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> if (parents.first() == Fixed.NULL_NODE_KEY.standardProperty) {
                wtx.insertObjectRecordAsLastChild(name, value).nodeKey
            } else {
                wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertObjectRecordAsLeftSibling(name, value).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            else -> throw AssertionError() //Should not happen
        }
        parents.removeFirst()
        parents.addFirst(wtx.parentKey)
        parents.addFirst(Fixed.NULL_NODE_KEY.standardProperty)
        if (wtx.kind === NodeKind.OBJECT || wtx.kind === NodeKind.ARRAY) {
            parents.removeFirst()
            parents.addFirst(key)
            parents.addFirst(Fixed.NULL_NODE_KEY.standardProperty)
        } else {
            adaptTrxPosAndStack(isNextTokenParentToken, key)
        }
    }

    private fun getObjectRecordValue(objVal: Any?): ObjectRecordValue<*> {
        val value: ObjectRecordValue<*>

        when (objVal) {
            is JsonObject -> {
                level++
                value = ObjectValue()
            }
            is JsonArray -> {
                level++
                value = ArrayValue()
            }
            is Boolean -> {
                value = BooleanValue(objVal)
            }
            is String -> {
                value = StringValue(objVal)
            }
            is NullValue -> {
                value = NullValue()
            }
            is Number -> {
                value = NumberValue(objVal)
            }
            else -> {
                throw AssertionError()
            }
        }
        return value
    }

    data class KeyValue(val field: String, val value: Any)
}