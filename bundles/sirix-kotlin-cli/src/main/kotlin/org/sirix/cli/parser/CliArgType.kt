package org.sirix.cli.parser

import kotlinx.cli.ArgType
import java.util.*

abstract class CliArgType<T: Any>(hasParameter: kotlin.Boolean)  : ArgType<T>(hasParameter) {


    object Uuid : CliArgType<UUID>(true) {
        override val description: kotlin.String
            get() = "{ xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx }"

        override fun convert(value: kotlin.String, name: kotlin.String): UUID = UUID.fromString(value)
    }
}