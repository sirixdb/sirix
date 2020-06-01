package org.sirix.cli.commands

import java.io.File


fun getTestFileCompletePath(fileName: String) : String {
    return  System.getProperty("java.io.tmpdir") + File.separator + fileName
}

