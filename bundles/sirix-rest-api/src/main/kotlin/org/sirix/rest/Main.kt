package org.sirix.rest

import io.vertx.core.Launcher

fun main() {
    Launcher.executeCommand("run", "-conf", "src/main/resources/sirix-conf.json", "org.sirix.rest.SirixVerticle")
}

