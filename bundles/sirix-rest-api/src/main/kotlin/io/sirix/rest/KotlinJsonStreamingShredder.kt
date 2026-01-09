package io.sirix.rest

import io.sirix.access.trx.node.json.objectvalue.*
import io.sirix.api.json.JsonNodeTrx
import io.sirix.node.NodeKind
import io.sirix.service.InsertPosition
import io.sirix.settings.Fixed
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.parsetools.JsonEventType
import io.vertx.core.parsetools.JsonParser
import it.unimi.dsi.fastutil.longs.LongArrayList
import java.util.*
import java.util.Objects.requireNonNull
import kotlin.collections.ArrayDeque

class KotlinJsonStreamingShredder(
    private val wtx: JsonNodeTrx,
    private val parser: JsonParser,
    private var insert: InsertPosition = InsertPosition.AS_FIRST_CHILD
) {
    private val parents = LongArrayList()

    private var level = 0

    fun call(): Future<Int> {
        return Future.future { promise ->
            try {
                parents.push(Fixed.NULL_NODE_KEY.standardProperty)
                val revision = wtx.revisionNumber
                val future = insertNewContent()
                future.onFailure(Throwable::printStackTrace)
                    .onSuccess { nodeKey ->
                        parser.endHandler {
                            processEndArrayOrEndObject(false)
                            wtx.moveTo(nodeKey)
                            promise.tryComplete(revision)
                        }
                    }
            } catch (t: Throwable) {
                promise.tryFail(t)
            }
        }
    }

    private fun insertNewContent(): Future<Long> {
        return Future.future { promise ->
            try {
                level = 0
                var insertedRootNodeKey = -1L
                var keyValue: KeyValue? = null
                var lastEndObjectOrEndArrayEventType: JsonEventType? = null
                val parentKind = ArrayDeque<JsonEventType>()

                // Helper to safely get the first parent kind (or null if empty)
                fun firstParentKind(): JsonEventType? = if (parentKind.isEmpty()) null else parentKind.first()
                
                // Iterate over all nodes.
                parser.handler { event ->
                    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                    when (event.type()) {
                        JsonEventType.START_OBJECT -> {
                            level++

                            if (keyValue != null) {
                                val value = keyValue!!.value
                                addObjectRecord(keyValue!!.field, value, value !is JsonObject && value !is JsonArray)
                                keyValue = null
                            }
                            val name = event.fieldName()
                            if (lastEndObjectOrEndArrayEventType != null) {
                                processEndArrayOrEndObject(name != null && firstParentKind() == JsonEventType.START_OBJECT)
                                lastEndObjectOrEndArrayEventType = null
                            }

                            if (name == null || firstParentKind() != JsonEventType.START_OBJECT) {
                                val insertedObjectNodeKey = addObject()

                                if (insertedRootNodeKey == -1L) {
                                    insertedRootNodeKey = insertedObjectNodeKey
                                    promise.tryComplete(insertedRootNodeKey)
                                }
                            } else {
                                keyValue = KeyValue(name, JsonObject())
                            }
                            parentKind.addFirst(event.type())
                        }
                        JsonEventType.VALUE -> {
                            val name = event.fieldName()
                            if (keyValue != null) {
                                val isNextTokenParentToken =
                                    name != null && firstParentKind() == JsonEventType.START_OBJECT
                                addObjectRecord(keyValue!!.field, keyValue!!.value, isNextTokenParentToken)
                                keyValue = null
                            }
                            if (lastEndObjectOrEndArrayEventType != null) {
                                processEndArrayOrEndObject(name != null)
                                lastEndObjectOrEndArrayEventType = null
                            }
                            val value = event.value()
                            if (name != null) {
                                keyValue = if (value == null) {
                                    KeyValue(name,
                                        NullValue()
                                    )
                                } else {
                                    KeyValue(name, value)
                                }
                            } else if (value is Number) {
                                val insertedNumberValueNodeKey = insertNumberValue(value)

                                if (insertedRootNodeKey == -1L) {
                                    insertedRootNodeKey = insertedNumberValueNodeKey
                                    promise.tryComplete(insertedRootNodeKey)
                                }
                            } else if (value is String) {
                                val insertedStringValueNodeKey = insertStringValue(value)

                                if (insertedRootNodeKey == -1L) {
                                    insertedRootNodeKey = insertedStringValueNodeKey
                                    promise.tryComplete(insertedRootNodeKey)
                                }
                            } else if (value is Boolean) {
                                val insertedBooleanValueNodeKey = insertBooleanValue(value)

                                if (insertedRootNodeKey == -1L) {
                                    insertedRootNodeKey = insertedBooleanValueNodeKey
                                    promise.tryComplete(insertedRootNodeKey)
                                }
                            } else if (value == null) {
                                val insertedNullValueNodeKey = insertNullValue()

                                if (insertedRootNodeKey == -1L) {
                                    insertedRootNodeKey = insertedNullValueNodeKey
                                    promise.tryComplete(insertedRootNodeKey)
                                }
                            }
                        }
                        JsonEventType.END_OBJECT, JsonEventType.END_ARRAY -> {
                            if (keyValue != null) {
                                addObjectRecord(
                                    keyValue!!.field,
                                    keyValue!!.value,
                                    event.type() == JsonEventType.END_OBJECT
                                )
                                keyValue = null
                            }
                            level--
                            if (lastEndObjectOrEndArrayEventType != null) {
                                processEndArrayOrEndObject(event.type() == JsonEventType.END_OBJECT)
                            }
                            lastEndObjectOrEndArrayEventType = event.type()
                            if (parentKind.isNotEmpty()) {
                                parentKind.removeFirst()
                            }
                        }
                        JsonEventType.START_ARRAY -> {
                            if (keyValue != null) {
                                val value = keyValue!!.value
                                addObjectRecord(
                                    keyValue!!.field,
                                    keyValue!!.value,
                                    value !is JsonObject && value !is JsonArray
                                )
                                keyValue = null
                            }

                            val name = event.fieldName()
                            if (lastEndObjectOrEndArrayEventType != null) {
                                processEndArrayOrEndObject(name != null && firstParentKind() == JsonEventType.START_OBJECT)
                                lastEndObjectOrEndArrayEventType = null
                            }

                            level++

                            if (name == null || firstParentKind() != JsonEventType.START_OBJECT) {
                                val insertedArrayNodeKey = insertArray()

                                if (insertedRootNodeKey == -1L) {
                                    insertedRootNodeKey = insertedArrayNodeKey
                                    promise.tryComplete(insertedRootNodeKey)
                                }
                            } else {
                                keyValue = KeyValue(name, JsonArray())
                            }
                            parentKind.addFirst(event.type())
                        }
                    }
                }
            } catch (t: Throwable) {
                promise.tryFail(t)
            }
        }
    }

    private fun processEndArrayOrEndObject(isNextTokenParentToken: Boolean) {
        if (parents.isEmpty) return
        parents.popLong()
        if (parents.isEmpty) return
        wtx.moveTo(parents.peekLong(0))
        if (isNextTokenParentToken) {
            if (parents.isEmpty) return
            parents.popLong()
            if (parents.isEmpty) return
            wtx.moveTo(parents.peekLong(0))
        }
    }

    private fun insertStringValue(stringValue: String): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (isParentNull) {
                    wtx.insertStringValueAsFirstChild(stringValue).nodeKey
                } else {
                    wtx.insertStringValueAsRightSibling(stringValue).nodeKey
                }
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (isParentNull) {
                    wtx.insertStringValueAsLastChild(stringValue).nodeKey
                } else {
                    wtx.insertStringValueAsRightSibling(stringValue).nodeKey
                }
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertStringValueAsLeftSibling(stringValue).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertStringValueAsRightSibling(stringValue).nodeKey
        }

        adaptTrxPosAndStack(false, key)

        return key
    }

    private fun insertBooleanValue(boolValue: Boolean): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (isParentNull) {
                    wtx.insertBooleanValueAsFirstChild(boolValue).nodeKey
                } else {
                    wtx.insertBooleanValueAsRightSibling(boolValue).nodeKey
                }
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (isParentNull) {
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
        val value = requireNonNull(numberValue)
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> if (isParentNull) {
                wtx.insertNumberValueAsFirstChild(value).nodeKey
            } else {
                wtx.insertNumberValueAsRightSibling(value).nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> if (isParentNull) {
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

    private fun adaptTrxPosAndStack(nextTokenIsParent: Boolean, key: Long) {
        if (!parents.isEmpty) {
            parents.popLong()
        }

        if (nextTokenIsParent && !parents.isEmpty) {
            wtx.moveTo(parents.peekLong(0))
        } else {
            parents.push(key)
        }
    }

    private fun insertNullValue(): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> if (isParentNull) {
                wtx.insertNullValueAsFirstChild().nodeKey
            } else {
                wtx.insertNullValueAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> if (isParentNull) {
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
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key: Long
        when (insert) {
            InsertPosition.AS_FIRST_CHILD -> key = if (isParentNull) {
                wtx.insertArrayAsFirstChild().nodeKey
            } else {
                wtx.insertArrayAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> key = if (isParentNull) {
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
        if (!parents.isEmpty) {
            parents.popLong()
        }
        parents.push(key)
        parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        return key
    }

    private fun addObject(): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key: Long
        when (insert) {
            InsertPosition.AS_FIRST_CHILD -> key = if (isParentNull) {
                wtx.insertObjectAsFirstChild().nodeKey
            } else {
                wtx.insertObjectAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> key = if (isParentNull) {
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
        if (!parents.isEmpty) {
            parents.popLong()
        }
        parents.push(key)
        parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        return key
    }

    private fun addObjectRecord(name: String?, objectValue: Any, isNextTokenParentToken: Boolean) {
        assert(name != null)
        val value: ObjectRecordValue<*> = getObjectRecordValue(objectValue)
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key: Long = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> if (isParentNull) {
                wtx.insertObjectRecordAsFirstChild(name, value).nodeKey
            } else {
                wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> if (isParentNull) {
                wtx.insertObjectRecordAsLastChild(name, value).nodeKey
            } else {
                wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertObjectRecordAsLeftSibling(name, value).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            else -> throw AssertionError() //Should not happen
        }
        if (!parents.isEmpty) {
            parents.popLong()
        }
        parents.push(wtx.parentKey)
        parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        if (wtx.kind === NodeKind.OBJECT || wtx.kind === NodeKind.ARRAY) {
            if (!parents.isEmpty) {
                parents.popLong()
            }
            parents.push(key)
            parents.push(Fixed.NULL_NODE_KEY.standardProperty)
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