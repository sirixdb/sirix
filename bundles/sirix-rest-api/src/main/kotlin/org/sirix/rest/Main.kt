package org.sirix.rest

import io.vertx.core.Launcher
import io.vertx.core.VertxOptions

fun main() {
    val launcher = Launcher()
    launcher.beforeStartingVertx(VertxOptions().setWorkerPoolSize(10).setMaxWorkerExecuteTime(300L * 1000 * 1000000))
    launcher.execute("run", "-conf", "sirix/bundles/sirix-rest-api/src/main/resources/sirix-conf.json", "org.sirix.rest.SirixVerticle")
}