# Bug: Native codec resource condition retained the old loader

- Fix / issue: S1 native range adapter, caught in pre-commit packaging review.
- Symptom: The bundled library resource was still conditional on SirixLZ77NativeDecoder although extraction had moved to NativeCodecLibrary. A native image reaching only the LZ4 adapter could omit its library and fall back to Java decoding.
- Trigger: Reach NativeCodecLibrary through FFILz4Compressor without reaching the old LZ77 decoder in native-image analysis.
- Root cause: The Java loader was factored out, but its class name in reachability metadata was not updated. Conditional resources follow the named type, not the extraction code's new owner.
- Why tests missed it: JVM runs load classpath resources without applying native-image reachability conditions. Native and old-library fallback JVM tests therefore passed.
- Regression test: NativeCodecLibraryTest.nativeImageResourceFollowsTheSharedLoader parses the packaged metadata and checks that the library follows the shared loader. The equivalent pre-fix metadata assertion recorded the old type instead of the expected shared loader; this is a packaging regression, not a claimed native-image executable reproduction.
- Lesson: [Move conditional native resources with their loader](../LESSONS.md#move-conditional-native-resources-with-their-loader). Check string-based reachability metadata when moving loading code.
