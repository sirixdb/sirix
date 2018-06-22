package org.sirix.rest

import io.vertx.core.Vertx

fun main(args: Array<String>) {
    val vertx = Vertx.vertx()
    vertx.deployVerticle(SirixVerticle::class.java.name)
}

