package org.sirix.cli.commands

import org.sirix.access.User

data class DataCommandOptions(val resourceName: String,
                              val data: String,
                              val datafile: String,
                              val commitMessage: String,
                              val user: User?) {
}