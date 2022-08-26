package org.sirix.cli.commands

import org.sirix.api.ResourceSession
import org.sirix.api.json.JsonResourceSession
import org.sirix.api.xml.XmlResourceSession
import org.sirix.cli.MetaDataEnum
import org.sirix.service.json.serialize.JsonRecordSerializer
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.service.xml.serialize.XmlSerializer
import java.io.ByteArrayOutputStream
import java.io.StringWriter

class SerializerAdapter(manager: ResourceSession<*, *>, nextTopLevelNodes: Int?) {

    private var jsonSerializerBuilder: JsonSerializer.Builder? = null
    private var jsonRecordSerializer: JsonRecordSerializer.Builder? = null
    private var xmlSerializerBuilder: XmlSerializer.XmlSerializerBuilder? = null

    private var outWriter: StringWriter? = null
    private var outStream: ByteArrayOutputStream? = null

    init {
        when (manager) {
            is JsonResourceSession -> {
                outWriter = StringWriter()
                if (nextTopLevelNodes == null) {
                    jsonSerializerBuilder = JsonSerializer.newBuilder(manager, outWriter)
                } else {
                    jsonRecordSerializer =
                        JsonRecordSerializer.newBuilder(manager, nextTopLevelNodes, outWriter)
                }
            }
            is XmlResourceSession -> {
                outStream = ByteArrayOutputStream()
                xmlSerializerBuilder =
                    XmlSerializer.XmlSerializerBuilder(manager, outStream)
            }
            else -> throw IllegalStateException("Unknown ResourceManager Type!")
        }
    }


    fun revisions(revisions: IntArray): SerializerAdapter {
        if (jsonSerializerBuilder != null) {
            jsonSerializerBuilder!!.revisions(revisions)

        } else if (jsonRecordSerializer != null) {
            jsonRecordSerializer!!.revisions(revisions)
        } else {
            xmlSerializerBuilder!!.revisions(revisions)
        }
        return this
    }

    fun startNodeKey(nodeKey: Long?): SerializerAdapter {
        if (nodeKey != null) {
            if (jsonSerializerBuilder != null) {
                jsonSerializerBuilder!!.startNodeKey(nodeKey)
            } else if (jsonRecordSerializer != null) {
                jsonRecordSerializer!!.startNodeKey(nodeKey)
            } else {
                xmlSerializerBuilder!!.startNodeKey(nodeKey)
            }
        }
        return this
    }

    fun metadata(metaData: MetaDataEnum): SerializerAdapter {
        if (jsonSerializerBuilder != null) {
            when (metaData) {
                MetaDataEnum.NODE_KEY_AND_CHILD_COUNT -> jsonSerializerBuilder!!.withNodeKeyAndChildCountMetaData(
                    true
                )
                MetaDataEnum.NODE_KEY -> jsonSerializerBuilder!!.withNodeKeyMetaData(true)
                MetaDataEnum.ALL -> jsonSerializerBuilder!!.withMetaData(true)
                else -> {
                    if (metaData != MetaDataEnum.NONE) {
                        throw IllegalArgumentException("Unkown Enum Type $metaData!")
                    }
                }
            }
        } else if (jsonRecordSerializer != null) {
            when (metaData) {
                MetaDataEnum.NODE_KEY_AND_CHILD_COUNT -> jsonRecordSerializer!!.withNodeKeyAndChildCountMetaData(
                    true
                )
                MetaDataEnum.NODE_KEY -> jsonRecordSerializer!!.withNodeKeyMetaData(true)
                MetaDataEnum.ALL -> jsonRecordSerializer!!.withMetaData(true)
                else -> {
                    if (metaData != MetaDataEnum.NONE) {
                        throw java.lang.IllegalArgumentException("Unkown Enum Type $metaData!")
                    }
                }
            }
        }
        return this
    }

    fun maxLevel(maxLevel: Long?): SerializerAdapter {
        if (maxLevel != null) {
            if (jsonSerializerBuilder != null) {
                jsonSerializerBuilder!!.maxLevel(maxLevel)
            } else if (jsonRecordSerializer != null) {
                jsonRecordSerializer!!.maxLevel(maxLevel)
            } else {
                xmlSerializerBuilder!!.maxLevel(maxLevel)
            }
        }
        return this
    }

    fun prettyPrint(prettyPrint: Boolean?): SerializerAdapter {
        if (prettyPrint != null && prettyPrint) {
            if (jsonSerializerBuilder != null) {
                jsonSerializerBuilder!!.prettyPrint()
            } else if (jsonRecordSerializer != null) {
                jsonRecordSerializer!!.prettyPrint()
            } else {
                xmlSerializerBuilder!!.prettyPrint()
            }
        }
        return this
    }

    fun serialize(): String {
        if (jsonSerializerBuilder != null) {
            jsonSerializerBuilder!!.build().call()
        } else if (jsonRecordSerializer != null) {
            jsonRecordSerializer!!.build().call()
        } else {
            xmlSerializerBuilder!!.emitIDs().emitRESTful().emitRESTSequence().build().call()
        }
        return if (outWriter != null) {
            outWriter.toString()
        } else {
            outStream.toString()
        }
    }

}
