package org.sirix.utils;

public final class OS {

  public static final boolean IS_64_BIT = is64Bit0();

  private OS() {
    throw new AssertionError();
  }

  /**
   * @return is the JVM 64-bit
   */
  public static boolean is64Bit() {
    return IS_64_BIT;
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
