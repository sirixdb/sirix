#!/bin/bash
#
# DEPRECATED: This script is deprecated and kept for reference only.
# Native-image configuration is now managed via Gradle and stored in:
#   bundles/sirix-query/src/main/resources/META-INF/native-image/io.sirix/query/
#
# To generate new metadata using the native-image agent:
#   ./gradlew :sirix-query:shadowJar
#   $JAVA_HOME/bin/java -agentlib:native-image-agent=config-output-dir=<output-dir> \
#     -jar bundles/sirix-query/build/libs/sirix-query-*-all.jar -iq
#

$JAVA_HOME/bin/java -agentlib:native-image-agent=config-output-dir=META-INF/native-image/io.sirix/query --enable-preview --add-modules=jdk.incubator.vector -DLOGGER_HOME=~/sirix-data --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED -jar ../bundles/sirix-query/build/libs/sirix-query-0.11.1-SNAPSHOT-all.jar -iq
