package org.sirix.cli.commands

import org.sirix.api.ResourceManager
import org.sirix.api.json.JsonResourceManager
import org.sirix.api.xml.XmlResourceManager
import org.sirix.cli.MetaDataEnum
import org.sirix.service.json.serialize.JsonRecordSerializer
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.service.xml.serialize.XmlSerializer
import java.io.ByteArrayOutputStream
import java.io.StringWriter

class QuerySerializerAdapter {

    var jsonSerializerBuilder: JsonSerializer.Builder? = null
    var jsonRecordSerializer: JsonRecordSerializer.Builder? = null
    var xmlSerializerBuilder: XmlSerializer.XmlSerializerBuilder? = null

    var outWriter: StringWriter? = null
    var outStream: ByteArrayOutputStream? = null

    constructor(manager: ResourceManager<*, *>, nextTopLevelNodes: Int?) {

        when (manager) {
            is JsonResourceManager -> {
                outWriter = StringWriter()
                if (nextTopLevelNodes == null) {
                    jsonSerializerBuilder = JsonSerializer.newBuilder(manager, outWriter)
                } else {
                    jsonRecordSerializer =
                        JsonRecordSerializer.newBuilder(manager, nextTopLevelNodes, outWriter)
                }
            }
            is XmlResourceManager -> {
                outStream = ByteArrayOutputStream()
                xmlSerializerBuilder =
                    XmlSerializer.XmlSerializerBuilder(manager, outStream)
            }
            else -> throw IllegalStateException("Unknown ResourceManager Type!")
        }
    }


    fun revisions(revisions: IntArray): QuerySerializerAdapter {
        if (jsonSerializerBuilder != null) {
            jsonSerializerBuilder!!.revisions(revisions)

        } else if (jsonRecordSerializer != null) {
            jsonRecordSerializer!!.revisions(revisions)
        } else {
            xmlSerializerBuilder!!.revisions(revisions)
        }
        return this
    }

    fun startNodeKey(nodeKey: Long?): QuerySerializerAdapter {
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

    fun metadata(metaData: MetaDataEnum): QuerySerializerAdapter {
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

    fun maxLevel(maxLevel: Long?): QuerySerializerAdapter {
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

    fun prettyPrint(prettyPrint: Boolean?): QuerySerializerAdapter {
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
        if (outWriter != null) {
            return outWriter.toString()
        } else {
            return outStream.toString()
        }
    }

}
