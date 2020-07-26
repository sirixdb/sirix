package org.sirix.cli.parser

import kotlinx.cli.ArgType
import java.time.LocalDateTime
import java.util.*

abstract class CliArgType<T : Any>(hasParameter: kotlin.Boolean) : ArgType<T>(hasParameter) {

    object Uuid : CliArgType<UUID>(true) {
        override val description: kotlin.String
            get() = "{ xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx }"

        override fun convert(value: kotlin.String, name: kotlin.String): UUID = UUID.fromString(value)
    }

    class Csv : ArgType<List<kotlin.String>>(true) {
        override val description: kotlin.String
            get() = "{ Comma separated List. \"a,b,c\" }"

        override fun convert(value: kotlin.String, name: kotlin.String): List<kotlin.String> =
            value.split(",").map { it.trim() }

    }

    class Timestamp : ArgType<LocalDateTime>(true) {
        override val description: kotlin.String
            get() = "{ 2020-12-20T18:44:39.464Z }"

        override fun convert(value: kotlin.String, name: kotlin.String): LocalDateTime = LocalDateTime.parse(value)
    }

    class Long : ArgType<kotlin.Long>(true) {
        override val description: kotlin.String
            get() = "{ Long }"

        override fun convert(value: kotlin.String, name: kotlin.String): kotlin.Long =
            value.toLongOrNull()
                ?: throw IllegalArgumentException("Option $name is expected to be long number. $value is provided.")
    }


}
