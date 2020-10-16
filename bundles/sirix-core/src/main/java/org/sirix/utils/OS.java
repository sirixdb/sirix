package org.sirix.utils;

public final class OS {

  public static final boolean IS_64_BIT = is64Bit0();

  private static final String OS = System.getProperty("os.name").toLowerCase();

  private static final boolean IS_LINUX = OS.startsWith("linux");

  private static final boolean IS_MAC = OS.contains("mac");

  private static final boolean IS_WIN = OS.startsWith("win");

  private static final boolean IS_WIN10 = OS.equals("windows 10");

  private OS() {
    throw new AssertionError();
  }

  /**
   * @return is the JVM 64-bit
   */
  public static boolean is64Bit() {
    return IS_64_BIT;
  }

  public static boolean isWindows() {
    return IS_WIN;
  }

  public static boolean isMacOSX() {
    return IS_MAC;
  }

  public static boolean isLinux() {
    return IS_LINUX;
  }

  private static boolean is64Bit0() {
    String systemProp;
    systemProp = System.getProperty("com.ibm.vm.bitmode");
    if (systemProp != null) {
      return "64".equals(systemProp);
    }
    systemProp = System.getProperty("sun.arch.data.model");
    if (systemProp != null) {
      return "64".equals(systemProp);
    }
    systemProp = System.getProperty("java.vm.version");
    return systemProp != null && systemProp.contains("_64");
  }
}
