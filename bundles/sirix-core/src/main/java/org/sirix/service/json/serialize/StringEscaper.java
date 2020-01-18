package org.sirix.service.json.serialize;

public final class StringEscaper {
  public static String escape(String value) {
    var returnVal = value.replace("\n", "\\n");
    returnVal = returnVal.replace("\t", "\\t");
    returnVal = returnVal.replace("\b", "\\b");
    returnVal = returnVal.replace("\r", "\\r");
    returnVal = returnVal.replace("\f", "\\f");
    return returnVal;
  }
}
