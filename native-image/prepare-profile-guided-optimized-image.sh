source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.2-graal 1>&2

# Performance tuning flags, optimization level 3, maximum inlining exploration, and compile for the architecture where the native image is generated.
NATIVE_IMAGE_OPTS="-O3 -H:TuneInlinerExploration=1 -march=native --gc=G1 --strict-image-heap --pgo=default.iprof --no-fallback"
   
NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS --enable-preview --initialize-at-build-time=ch.qos.logback --initialize-at-build-time=org.slf4j.impl.StaticLoggerBinder --initialize-at-build-time=org.slf4j.LoggerFactory --initialize-at-build-time=org.slf4j.helpers.NOPLoggerFactory --initialize-at-build-time=org.slf4j.helpers.SubstituteLoggerFactory"
    
NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS -DLOGGER_HOME=~/sirix-data --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
    
native-image $NATIVE_IMAGE_OPTS -cp ../bundles/sirix-query/build/libs/sirix-query-0.11.0-all.jar:. -o sirix-shell io.sirix.query.Main
./sirix-shell
