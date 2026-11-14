package io.sirix.rest

import io.vertx.launcher.application.VertxApplication
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mockStatic

@Tag("offline")
class MainTest {
    @Test
    fun `main forwards all launcher arguments`() {
        val arguments = arrayOf("io.sirix.rest.SirixVerticle", "-conf", "/tmp/operator-config.json", "-instances", "2")
        mockStatic(VertxApplication::class.java).use { launcher ->
            main(arguments)
            launcher.verify { VertxApplication.main(arguments) }
            launcher.verifyNoMoreInteractions()
        }
    }
}
