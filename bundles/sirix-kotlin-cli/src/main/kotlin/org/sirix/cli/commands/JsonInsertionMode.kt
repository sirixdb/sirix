package org.sirix.cli.commands

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import org.sirix.access.trx.node.json.objectvalue.*
import org.sirix.api.json.JsonNodeTrx
import org.sirix.service.json.JsonNumber
import java.io.IOException

enum class JsonInsertionMode {
    AS_FIRST_CHILD {
        override fun insertSubtree(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertSubtreeAsFirstChild(jsonReader)
        }

        override fun insertString(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertStringValueAsFirstChild(jsonReader.nextString())
            wtx.commit()
        }

        override fun insertNumber(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertNumberValueAsFirstChild(JsonNumber.stringToNumber(jsonReader.nextString()))
            wtx.commit()
        }

        override fun insertNull(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            jsonReader.nextNull()
            wtx.insertNullValueAsFirstChild()
            wtx.commit()
        }

        override fun insertBoolean(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertBooleanValueAsFirstChild(jsonReader.nextBoolean())
            wtx.commit()
        }

        override fun insertObjectRecord(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertObjectRecordAsFirstChild(jsonReader.nextName(), getObjectRecordValue(jsonReader))
            wtx.commit()
        }
    },
    AS_RIGHT_SIBLING {
        override fun insertSubtree(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertSubtreeAsRightSibling(jsonReader)
        }

        override fun insertString(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertStringValueAsRightSibling(jsonReader.nextString())
            wtx.commit()
        }

        override fun insertNumber(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertNumberValueAsRightSibling(JsonNumber.stringToNumber(jsonReader.nextString()))
            wtx.commit()
        }

        override fun insertNull(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            jsonReader.nextNull()
            wtx.insertNullValueAsRightSibling()
            wtx.commit()
        }

        override fun insertBoolean(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertBooleanValueAsRightSibling(jsonReader.nextBoolean())
            wtx.commit()
        }

        override fun insertObjectRecord(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertObjectRecordAsRightSibling(jsonReader.nextName(), getObjectRecordValue(jsonReader))
            wtx.commit()
        }
    };

    @Throws(IOException::class)
    fun getObjectRecordValue(jsonReader: JsonReader): ObjectRecordValue<*>? {
        val nextToken: JsonToken = jsonReader.peek()
        val value: ObjectRecordValue<*>
        value = when (nextToken) {
            JsonToken.BEGIN_OBJECT -> {
                jsonReader.beginObject()
                ObjectValue()
            }
            JsonToken.BEGIN_ARRAY -> {
                jsonReader.beginArray()
                ArrayValue()
            }
            JsonToken.BOOLEAN -> {
                val booleanVal: Boolean = jsonReader.nextBoolean()
                BooleanValue(booleanVal)
            }
            JsonToken.STRING -> {
                val stringVal: String = jsonReader.nextString()
                StringValue(stringVal)
            }
            JsonToken.NULL -> {
                jsonReader.nextNull()
                NullValue()
            }
            JsonToken.NUMBER -> {
                val numberVal: Number = JsonNumber.stringToNumber(jsonReader.nextString())
                NumberValue(numberVal)
            }
            JsonToken.END_ARRAY, JsonToken.END_DOCUMENT, JsonToken.END_OBJECT, JsonToken.NAME -> throw AssertionError()
            else -> throw AssertionError()
        }
        return value
    }

    abstract fun insertSubtree(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertString(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertNumber(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertNull(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertBoolean(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertObjectRecord(wtx: JsonNodeTrx, jsonReader: JsonReader)

    companion object {
        fun getInsertionModeByName(name: String) = valueOf(name.replace('-', '_').toUpperCase())
    }
}
