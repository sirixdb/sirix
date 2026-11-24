package io.sirix.pit;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import org.jspecify.annotations.Nullable;
import org.pitest.reloc.asm.ClassReader;
import org.pitest.reloc.asm.ClassVisitor;
import org.pitest.reloc.asm.ClassWriter;
import org.pitest.reloc.asm.MethodVisitor;
import org.pitest.reloc.asm.Opcodes;

/**
 * Adapts the pinned PIT executor: its listener API is downstream of a submission-order wait and
 * cannot itself preserve later completed units. PIT inherits agents into minions, whose client
 * classpath excludes the executor; those JVMs need no adapter.
 */
public final class StreamingCompletionAgent implements ClassFileTransformer {
  private static final String EXECUTOR = "org/pitest/mutationtest/execute/MutationAnalysisExecutor";
  private boolean installed;

  /** Installs and verifies the pinned executor adapter before analysis can start. */
  public static void premain(final @Nullable String arguments, final Instrumentation instrumentation)
      throws ClassNotFoundException {
    if (StreamingCompletionAgent.class.getClassLoader().getResource(EXECUTOR + ".class") == null) {
      return; // PIT minions have no controlling executor on their client classpath.
    }
    final StreamingCompletionAgent agent = new StreamingCompletionAgent();
    instrumentation.addTransformer(agent);
    Class.forName(EXECUTOR.replace('/', '.'), false, StreamingCompletionAgent.class.getClassLoader());
    if (!agent.installed) {
      throw new IllegalStateException("Cannot install completion-order reporting for the pinned PIT executor");
    }
    instrumentation.removeTransformer(agent);
  }

  @Override
  public byte @Nullable [] transform(final @Nullable ClassLoader loader, final @Nullable String className,
      final @Nullable Class<?> redefined, final @Nullable ProtectionDomain domain, final byte[] bytes) {
    if (!EXECUTOR.equals(className)) {
      return null;
    }
    final ClassReader reader = new ClassReader(bytes);
    final ClassWriter writer = new ClassWriter(reader, 0);
    reader.accept(new ClassVisitor(Opcodes.ASM9, writer) {
      @Override
      public @Nullable MethodVisitor visitMethod(final int access, final String name, final String descriptor,
          final @Nullable String signature, final String @Nullable [] exceptions) {
        final MethodVisitor method = super.visitMethod(access, name, descriptor, signature, exceptions);
        if (!name.equals("run") || !descriptor.equals("(Ljava/util/List;)V")) {
          return method;
        }
        method.visitCode();
        field(method, "executor", "Ljava/util/concurrent/ThreadPoolExecutor;");
        field(method, "resultInterceptor", "Lorg/pitest/mutationtest/MutationResultInterceptor;");
        field(method, "listeners", "Ljava/util/List;");
        method.visitVarInsn(Opcodes.ALOAD, 1);
        method.visitMethodInsn(Opcodes.INVOKESTATIC, "io/sirix/pit/CompletionOrderAnalysis", "run",
            "(Ljava/util/concurrent/ThreadPoolExecutor;Lorg/pitest/mutationtest/MutationResultInterceptor;"
                + "Ljava/util/List;Ljava/util/List;)V",
            false);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(4, 2);
        method.visitEnd();
        installed = true;
        return null;
      }
    }, 0);
    return writer.toByteArray();
  }

  private static void field(final MethodVisitor method, final String name, final String descriptor) {
    method.visitVarInsn(Opcodes.ALOAD, 0);
    method.visitFieldInsn(Opcodes.GETFIELD, EXECUTOR, name, descriptor);
  }
}
