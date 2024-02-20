#!/bin/bash
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.2-graal 1>&2

# ./mvnw clean verify removes target/ and will re-trigger native image creation.
if [ ! -f sirix-query-0.10.6-all ]; then

    # Performance tuning flags, optimization level 3, maximum inlining exploration, and compile for the architecture where the native image is generated.
    NATIVE_IMAGE_OPTS="-O3 -H:TuneInlinerExploration=1 -march=native --gc=G1 --strict-image-heap --pgo-instrument --no-fallback"
   
    NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS --enable-preview --initialize-at-build-time=ch.qos.logback --initialize-at-build-time=org.slf4j.impl.StaticLoggerBinder --initialize-at-build-time=org.slf4j.LoggerFactory --initialize-at-build-time=org.slf4j.helpers.NOPLoggerFactory --initialize-at-build-time=org.slf4j.helpers.SubstituteLoggerFactory"
    
    NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS -DLOGGER_HOME=~/sirix-data --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
    
    native-image $NATIVE_IMAGE_OPTS -jar ../bundles/sirix-query/build/libs/sirix-query-0.10.6-all.jar sirix-shell
fi
