package io.sirix.rest

import io.vertx.launcher.application.VertxApplication

// Add neccessary VM options to run config: --enable-preview --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
// Use keycloak container instead of local keycloak instance: https://github.com/sirixdb/sirix/blob/master/.github/workflows/gradle.yml#L53-L60
fun main(args: Array<String>) {
    VertxApplication.main(arrayOf(
        "io.sirix.rest.SirixVerticle",
        "-conf", "/home/johannes/IdeaProjects/sirix/bundles/sirix-rest-api/src/main/resources/sirix-conf.json"
    ))
}
