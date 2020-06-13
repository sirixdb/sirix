package org.sirix.cli.commands

import org.sirix.access.User
import org.sirix.cli.CliOptions
import org.sirix.service.json.serialize.StringValue

class DumpResourceHistoryCommand(options: CliOptions, val resourceName: String, private val user: User?): CliCommand(options) {

    override fun execute() {

        val database = openDatabase(user)
        database.use {
            val buffer = StringBuilder()
            val manager = database.openResourceManager(resourceName)

            manager.use {

                val historyList = manager.getHistory()

                buffer.append("{\"history\":[")

                historyList.forEachIndexed { index, revisionTuple ->
                    buffer.append("{\"revision\":")
                    buffer.append(revisionTuple.revision)
                    buffer.append(",")

                    buffer.append("\"revisionTimestamp\":\"")
                    buffer.append(revisionTuple.revisionTimestamp)
                    buffer.append("\",")

                    buffer.append("\"author\":\"")
                    buffer.append(StringValue.escape(revisionTuple.user.name))
                    buffer.append("\",")

                    buffer.append("\"commitMessage\":\"")
                    buffer.append(StringValue.escape(revisionTuple.commitMessage.orElse("")))
                    buffer.append("\"}")

                    if (index != historyList.size - 1)
                        buffer.append(",")
                }

                buffer.append("]}")
            }
            cliPrinter.prnLn(buffer.toString())
        }
    }

}