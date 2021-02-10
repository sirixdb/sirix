/*
 * [New BSD License] Copyright (c) 2011-2012, Brackit Project Team <info@brackit.org> All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the Brackit Project Team nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.node;

import java.util.Arrays;

import org.sirix.exception.SirixException;
import org.sirix.node.interfaces.SimpleDeweyID;

/**
 * @author Michael Haustein
 * @author Christian Mathis
 * @author Sebastian Baechle
 */
public final class SirixDeweyID implements Comparable<SirixDeweyID>, SimpleDeweyID {

  private final static String divisionSeparator = ".";

  private final static int attributeRootDivisionValue = 1;

  private final static int recordValueRootDivisionValue = 0;

  // must be an even number! when a new DeweyID is calculated, and there is a
  // choice, DISTANCE_TO_SIBLING/2 nodes fits between the existing node, and
  // the new node. For example: id1=1.7, id2=NULL; new ID will be
  // 1.7+DISTANCE_TO_SIBLING
  private static int distanceToSibling = 16;

  private final static int namespaceRootDivisionValue = 0;

  private final int[] divisionValues;
  private final int level;

  // possible bitlength for one division
  // private final static byte[] divisionLengthArray =
  // {3,4,6,8,12,16,20,24,31};

  private final static byte[] divisionLengthArray = { 7, 14, 21, 28, 31 };

  private final static boolean[][] bitStringAsBoolean =
      { { false }, { true, false }, { true, true, false }, { true, true, true, false }, { true, true, true, true } };

  // the maximum divisionvalue for the corresponding length
  // private final static int[] maxDivisionValue =
  // {8,24,88,344,4440,69976,1118552,17895768, 2147483647};
  private final static int[] maxDivisionValue = new int[divisionLengthArray.length];

  // the complete bitlength which is needed to store a value
  // private final static byte[] completeDivisionLengthArray =
  // {5,7,9,12,16,21,25,29,36};
  private final static int[] completeDivisionLengthArray = new int[divisionLengthArray.length];

  // the first DeweyID, for the corresponding position in
  // binaryTreeSearchArray
  // private final static int[] binaryTreeSuffixInit={
  // 0,0,0,0,1,0,0,0,0,0,0,9,25,0,0,0,0,0,0,0,0,0,4441,69977,1118553,17895769,0,89,345};
  private final static int[] binaryTreeSuffixInit;

  // the length, for the Prefix; the position is calculated by the formuala
  // 2*i+2 for a 1, and 2*i+1 for a 0
  // private final static byte[] binaryTreeSearchArray={
  // 0,0,0,0,3,0,0,0,0,0,0,4,6,0,0,0,0,0,0,0,0,0,16,20,24,31,0,8,12,0,0,0,0,0,0,0,0
  // };
  private final static byte[] binaryTreeSearchArray;

  static {
    // calculates the maxDivisionValues
    for (int i = 0; i < divisionLengthArray.length; i++) {
      maxDivisionValue[i] = 1 << divisionLengthArray[i];

      /* for 0-reasons the 000 cannot be used
       * Because Division-Value 0 is allowed
       */
      if (i == 0)
        maxDivisionValue[i] -= 1;

      if (i != 0) {
        if (maxDivisionValue[i] < 0) {
          // if maxDivisionValue is negative, the Integer.MAX_VALUE
          // can be stored with these bits
          maxDivisionValue[i] = Integer.MAX_VALUE;
        } else {
          maxDivisionValue[i] += maxDivisionValue[i - 1];
        }
      }
    }

    if (maxDivisionValue[divisionLengthArray.length - 1] != Integer.MAX_VALUE) {
      throw new SirixException(
          "DeweyID: It is not possible to handle all positive Integer values with the given divisionLengthArray!");
    }

    // check if bitStringAsBoolean has as many rows as divisionLengthArray
    if (bitStringAsBoolean.length != divisionLengthArray.length) {
      throw new SirixException(
          "DeweyID: bitStringAsBoolean and divisionLengthArray must have equal rows!");
    }

    // now initialize the binaryTreeSuffixInit(this is the first Division
    // for the corresponding row)
    int maxBitStringLength = 0;
    for (int i = 0; i < bitStringAsBoolean.length; i++) {
      if (bitStringAsBoolean[i].length > maxBitStringLength) {
        maxBitStringLength = bitStringAsBoolean[i].length;
      }
    }

    // calculate the max index which can appear
    int index = 0;
    for (int i = 0; i < maxBitStringLength; i++) {
      index = (2 * index) + 2;
    }

    // init the binarySearchTrees
    binaryTreeSuffixInit = new int[index + 1];
    binaryTreeSearchArray = new byte[index + 1];

    for (int i = 0; i < bitStringAsBoolean.length; i++) {
      index = 0;
      for (int j = 0; j < bitStringAsBoolean[i].length; j++) {
        if (bitStringAsBoolean[i][j] == true) {
          index = (2 * index) + 2;
        } else {
          index = (2 * index) + 1;
        }

        if (binaryTreeSuffixInit[index] != 0) {
          throw new SirixException("DeweyID: The bitStringAsBoolean is not prefixfree!");
        }
      }
      if (i == 0) {
        // the first row begin with 1
        // binaryTreeSuffixInit[index] = 1;
        // but we have to begin at 000 for 0-reasons
        binaryTreeSuffixInit[index] = 0;

        // because the 0-value is allowed
        binaryTreeSuffixInit[index] -= 1;
      } else {
        // all other rows begin after the max value of the row before
        binaryTreeSuffixInit[index] = maxDivisionValue[i - 1] + 1;
      }

      binaryTreeSearchArray[index] = divisionLengthArray[i];
    }

    for (int i = 0; i < bitStringAsBoolean.length; i++) {
      completeDivisionLengthArray[i] = bitStringAsBoolean[i].length + divisionLengthArray[i];
    }

  }

  private final int[] parseDivisionValues(String divisionPart) {
    if (divisionPart.charAt(divisionPart.length() - 1) != '.')
      divisionPart += '.';

    String[] divisions = divisionPart.split("\\.");
    int[] divisionValues = new int[divisions.length];
    int i = 0;

    for (String division : divisions) {
      try {
        divisionValues[i] = Integer.parseInt(division);
        i++;
      } catch (NumberFormatException e) {
        throw new SirixException("Division " + i + " has an invalid value: " + division, e);
      }
    }

    return divisionValues;
  }

  private int calcLevel(int[] divisionValues) {
    int level = 0;
    for (int i = 0; i < divisionValues.length; i++) {
      if (divisionValues[i] % 2 == 1)
        level++;
    }
    return level;
  }

  public SirixDeweyID(byte[] deweyIDbytes) {
    int division = 1;
    int currentLevel = 1;

    int[] tempDivision = new int[10];
    int tempDivisionLength = 0;

    // the first division "1" is implicit there and not encoded in
    // deweyIDbytes
    tempDivision[tempDivisionLength++] = 1;

    // first scan deweyID bytes to get length

    int binaryTreeSearchIndex = 0;
    int helpFindingBit;
    boolean prefixBit = true;
    int suffixlength = 0;
    int suffix = 0;
    // parse complete byte-Array
    for (int bitIndex = 0; bitIndex < (8 * deweyIDbytes.length); bitIndex++) {

      switch (bitIndex % 8) {
        case 0:
          helpFindingBit = 128;
          break;
        case 1:
          helpFindingBit = 64;
          break;
        case 2:
          helpFindingBit = 32;
          break;
        case 3:
          helpFindingBit = 16;
          break;
        case 4:
          helpFindingBit = 8;
          break;
        case 5:
          helpFindingBit = 4;
          break;
        case 6:
          helpFindingBit = 2;
          break;
        default:
          helpFindingBit = 1;
          break;
      }

      if (prefixBit) { // still parsing the prefix
        if ((deweyIDbytes[bitIndex / 8] & helpFindingBit) == helpFindingBit) {
          // bit is set
          // binaryTreeSearchIndex = (((2 * binaryTreeSearchIndex) + 2));
          binaryTreeSearchIndex = (((binaryTreeSearchIndex << 1) + 2));
        } else { // bit is not set
          // binaryTreeSearchIndex = (((2 * binaryTreeSearchIndex) + 1));
          binaryTreeSearchIndex = (((binaryTreeSearchIndex << 1) + 1));
        }

        if ((binaryTreeSearchArray.length > binaryTreeSearchIndex) && (binaryTreeSearchArray[binaryTreeSearchIndex]
            != 0)) {
          // division found;
          prefixBit = false; // memorize we found the complete prefix
          suffixlength = binaryTreeSearchArray[binaryTreeSearchIndex];
          // initialize suffix
          suffix = binaryTreeSuffixInit[binaryTreeSearchIndex];
        }
      } else { // prefix already found, so we are calculating the suffix
        if ((deweyIDbytes[bitIndex / 8] & helpFindingBit) == helpFindingBit) {
          // bit is set
          suffix += 1 << suffixlength - 1;
        }

        suffixlength--;
        if (suffixlength == 0) {
          // -1 is not a valid Divisionvalue
          if (suffix != -1) {
            division++;

            if (tempDivisionLength == tempDivision.length) {
              int[] newTempDivision = new int[tempDivisionLength + 5];
              System.arraycopy(tempDivision, 0, newTempDivision, 0, tempDivisionLength);
              tempDivision = newTempDivision;
            }

            tempDivision[tempDivisionLength++] = suffix;
            if (suffix % 2 == 1)
              currentLevel++;
          }

          prefixBit = true;
          binaryTreeSearchIndex = 0;

        }
      }
    }

    this.level = currentLevel;
    this.divisionValues = new int[division];
    System.arraycopy(tempDivision, 0, divisionValues, 0, division);
  }

  public SirixDeweyID(byte[] deweyIDbytes, int offset, int length) {
    int division = 1;
    int currentLevel = 1;

    int[] tempDivision = new int[10];
    int tempDivisionLength = 0;

    // the first division "1" is implicit there and not encoded in
    // deweyIDbytes
    tempDivision[tempDivisionLength++] = 1;

    // first scan deweyID bytes to get length

    int binaryTreeSearchIndex = 0;
    int helpFindingBit;
    boolean prefixBit = true;
    int suffixlength = 0;
    int suffix = 0;
    // parse complete byte-Array

    int end = 8 * length;

    for (int bitIndex = 0; bitIndex < end; bitIndex++) {

      switch (bitIndex % 8) {
        case 0:
          helpFindingBit = 128;
          break;
        case 1:
          helpFindingBit = 64;
          break;
        case 2:
          helpFindingBit = 32;
          break;
        case 3:
          helpFindingBit = 16;
          break;
        case 4:
          helpFindingBit = 8;
          break;
        case 5:
          helpFindingBit = 4;
          break;
        case 6:
          helpFindingBit = 2;
          break;
        default:
          helpFindingBit = 1;
          break;
      }

      if (prefixBit) { // still parsing the prefix
        if ((deweyIDbytes[offset + bitIndex / 8] & helpFindingBit) == helpFindingBit) {
          // bit is set
          binaryTreeSearchIndex = (((2 * binaryTreeSearchIndex) + 2));
        } else { // bit is not set
          binaryTreeSearchIndex = (((2 * binaryTreeSearchIndex) + 1));
        }

        if ((binaryTreeSearchArray.length > binaryTreeSearchIndex) && (binaryTreeSearchArray[binaryTreeSearchIndex]
            != 0)) {
          // division found;
          prefixBit = false; // memorize we found the complete prefix
          suffixlength = binaryTreeSearchArray[binaryTreeSearchIndex];
          // initialize suffix
          suffix = binaryTreeSuffixInit[binaryTreeSearchIndex];
        }
      } else { // prefix already found, so we are calculating the suffix
        if ((deweyIDbytes[offset + bitIndex / 8] & helpFindingBit) == helpFindingBit) {
          // bit is set
          suffix += 1 << suffixlength - 1;
        }

        suffixlength--;
        if (suffixlength == 0) {
          // -1 is not a valid Divisionvalue
          if (suffix != -1) {
            division++;

            if (tempDivisionLength == tempDivision.length) {
              int[] newTempDivision = new int[tempDivisionLength + 5];
              System.arraycopy(tempDivision, 0, newTempDivision, 0, tempDivisionLength);
              tempDivision = newTempDivision;
            }

            tempDivision[tempDivisionLength++] = suffix;
            if (suffix % 2 == 1)
              currentLevel++;
          }

          prefixBit = true;
          binaryTreeSearchIndex = 0;

        }
      }
    }

    this.level = currentLevel;
    this.divisionValues = new int[division];
    System.arraycopy(tempDivision, 0, divisionValues, 0, division);
  }

  public SirixDeweyID(int[] divisionValues) {
    this.divisionValues = Arrays.copyOf(divisionValues, divisionValues.length);
    this.level = calcLevel(this.divisionValues);
  }

  public SirixDeweyID(int[] divisionValues, int level) {
    this.divisionValues = Arrays.copyOf(divisionValues, divisionValues.length);
    this.level = level;
  }

  public SirixDeweyID(int length, int[] divisionValues) {
    this.divisionValues = Arrays.copyOf(divisionValues, length);
    this.level = calcLevel(this.divisionValues);
  }

  public SirixDeweyID(int length, int[] divisionValues, int level) {
    this.divisionValues = Arrays.copyOf(divisionValues, length);
    this.level = level;
  }

  public SirixDeweyID(SirixDeweyID deweyID, int extraDivisionValue) {
    this.divisionValues = new int[deweyID.divisionValues.length + 1];
    if (extraDivisionValue == recordValueRootDivisionValue) {
      this.level = deweyID.level;
    } else {
      this.level = deweyID.level + 1;
    }
    System.arraycopy(deweyID.divisionValues, 0, divisionValues, 0, deweyID.divisionValues.length);
    divisionValues[divisionValues.length - 1] = extraDivisionValue;
  }

  public SirixDeweyID(String deweyID) {
    this.divisionValues = parseDivisionValues(deweyID);
    this.level = calcLevel(divisionValues);
  }

  public int getLevel() {
    return level - 1;
  }

  public int getNumberOfDivisions() {
    return divisionValues.length;
  }

  public int[] getDivisionValues() {
    return divisionValues;
  }

  public int getDivisionValue(int division) {
    if (division >= divisionValues.length) {
      throw new SirixException("Invalid division: " + division);
    }
    return divisionValues[division];
  }

  /**
   * Calculates the number of bits, that are needed to store the choosen
   * division-value
   */
  private int getDivisionBits(int division) {
    if (divisionValues[division] <= maxDivisionValue[0])
      return completeDivisionLengthArray[0];
    else if (divisionValues[division] <= maxDivisionValue[1])
      return completeDivisionLengthArray[1];
    else if (divisionValues[division] <= maxDivisionValue[2])
      return completeDivisionLengthArray[2];
    else if (divisionValues[division] <= maxDivisionValue[3])
      return completeDivisionLengthArray[3];
    else if (divisionValues[division] <= maxDivisionValue[4])
      return completeDivisionLengthArray[4];
    else if (divisionValues[division] <= maxDivisionValue[5])
      return completeDivisionLengthArray[5];
    else if (divisionValues[division] <= maxDivisionValue[6])
      return completeDivisionLengthArray[6];
    else if (divisionValues[division] <= maxDivisionValue[7])
      return completeDivisionLengthArray[7];
    else
      return completeDivisionLengthArray[8];
  }

  /** 
   * sets the bits in the byteArray for the given division, which has to write
   * its bits at position bitIndex
   * returns the bitIndex where the next Division can start
   */
  private final int setDivisionBitArray(int[] divisionValues, byte[] byteArray, int division,
      int bitIndex) {
    int divisionSize = getDivisionBits(division);
    int prefixLength;
    int suffix;
    boolean[] prefix;

    prefixLength = divisionLengthArray[divisionLengthArray.length - 1];
    prefix = bitStringAsBoolean[divisionLengthArray.length - 1];
    suffix = divisionValues[division] - maxDivisionValue[divisionLengthArray.length - 2] - 1;

    for (int i = 0; i < divisionLengthArray.length - 2; i++) {
      if (divisionValues[division] <= maxDivisionValue[i]) {
        prefixLength = divisionLengthArray[i];
        prefix = bitStringAsBoolean[i];
        if (i != 0) {
          suffix = divisionValues[division] - maxDivisionValue[i - 1] - 1;
        } else {
          suffix = divisionValues[division] + 1;
        }
        break;
      }
    }

    // set the prefixbits
    for (int i = 0; i < prefix.length; i++) {
      if (prefix[i] == true) {
        byteArray[bitIndex / 8] |= (int) Math.pow(2, 7 - (bitIndex % 8));
      }
      bitIndex++;
    }

    // calculate the rest of the bits
    for (int i = 1; i <= divisionSize - prefix.length; i++) {
      int k = 1;
      k = k << divisionSize - prefix.length - i;
      if (suffix >= k) {
        suffix -= k;
        byteArray[bitIndex / 8] |= (int) Math.pow(2, 7 - (bitIndex % 8));
      }
      bitIndex++;
    }
    return bitIndex;
  }

  public byte[] toBytes() {
    return toBytes(divisionValues);
  }

  public byte[] toAttributeRootBytes() {
    int[] attRootDivisions = Arrays.copyOf(divisionValues, divisionValues.length + 1);
    attRootDivisions[attRootDivisions.length - 1] = 1;
    return toBytes(attRootDivisions);
  }

  private byte[] toBytes(int[] divisionValues) {
    // calculate needed bits for deweyID
    int numberOfDivisionBits = 0;

    // starting at second division, because the first "1" can be optimized
    for (int i = 1; i < divisionValues.length; i++) {
      if (divisionValues[i] <= maxDivisionValue[0])
        numberOfDivisionBits += completeDivisionLengthArray[0];
      else if (divisionValues[i] <= maxDivisionValue[1])
        numberOfDivisionBits += completeDivisionLengthArray[1];
      else if (divisionValues[i] <= maxDivisionValue[2])
        numberOfDivisionBits += completeDivisionLengthArray[2];
      else if (divisionValues[i] <= maxDivisionValue[3])
        numberOfDivisionBits += completeDivisionLengthArray[3];
      else if (divisionValues[i] <= maxDivisionValue[4])
        numberOfDivisionBits += completeDivisionLengthArray[4];
      else if (divisionValues[i] <= maxDivisionValue[5])
        numberOfDivisionBits += completeDivisionLengthArray[5];
      else if (divisionValues[i] <= maxDivisionValue[6])
        numberOfDivisionBits += completeDivisionLengthArray[6];
      else if (divisionValues[i] <= maxDivisionValue[7])
        numberOfDivisionBits += completeDivisionLengthArray[7];
      else
        numberOfDivisionBits += completeDivisionLengthArray[8];
    }

    byte[] deweyIDbytes;
    if (numberOfDivisionBits % 8 == 0) {
      deweyIDbytes = new byte[(numberOfDivisionBits / 8)];
    } else {
      deweyIDbytes = new byte[(numberOfDivisionBits / 8) + 1];
    }

    int bitIndex = 0;
    for (int i = 1; i < this.divisionValues.length; i++) {
      bitIndex = setDivisionBitArray(divisionValues, deweyIDbytes, i, bitIndex);
    }

    return deweyIDbytes;
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder();

    for (int i = 0; i < divisionValues.length; i++) {
      if (i != 0) {
        out.append(SirixDeweyID.divisionSeparator);
      }
      out.append(divisionValues[i]);
    }

    return out.toString();
  }

  @Override
  public int compareTo(SirixDeweyID deweyID) {
    if (this == deweyID) {
      return 0;
    }

    int[] myD = this.divisionValues;
    int[] oD = deweyID.divisionValues;
    int myLen = myD.length;
    int oLen = oD.length;
    int len = ((myLen <= oLen) ? myLen : oLen);

    int pos = -1;
    while (++pos < len) {
      if (myD[pos] != oD[pos]) {
        return myD[pos] - oD[pos];
      }
    }

    return (myLen == oLen) ? 0 : (myLen < oLen) ? -1 : 1;
  }

  @Override
  public boolean equals(Object object) {
    return ((object instanceof SirixDeweyID) && (compareTo((SirixDeweyID) object) == 0));
  }

  public static int compare(byte[] deweyID1, byte[] deweyID2) {
    int length1 = deweyID1.length;
    int length2 = deweyID2.length;
    int length = ((length1 <= length2) ? length1 : length2);

    int pos = -1;
    while (++pos < length) {
      int v2 = deweyID2[pos] & 255;
      int v1 = deweyID1[pos] & 255;

      if (v1 != v2) {
        return v1 - v2;
      }
    }

    return length1 - length2;
  }

  public static int compareAsPrefix(byte[] deweyID1, byte[] deweyID2) {
    int length1 = deweyID1.length;
    int length2 = deweyID2.length;
    int length = ((length1 <= length2) ? length1 : length2);

    int pos = -1;
    while (++pos < length) {
      int v2 = deweyID2[pos] & 255;
      int v1 = deweyID1[pos] & 255;

      if (v1 != v2) {
        return v1 - v2;
      }
    }

    return (length1 <= length2) ? 0 : 1;
  }

  public boolean isSelfOf(SirixDeweyID deweyID) {
    return compareTo(deweyID) == 0;
  }

  public boolean isAttributeOf(SirixDeweyID deweyID) {
    int[] myD = divisionValues;
    int[] oD = deweyID.divisionValues;
    int myLen = myD.length;
    int oLen = oD.length;

    if (oLen != myLen - 2) {
      return false;
    }

    int len = oLen;
    int pos = -1;
    while (++pos < len) {
      if (myD[pos] != oD[pos]) {
        return false;
      }
    }

    return ((myD[myLen - 2] == 1) && (myD[myLen - 1] % 2 != 0));
  }

  public boolean isAncestorOf(SirixDeweyID deweyID) {
    int[] myD = divisionValues;
    int[] oD = deweyID.divisionValues;
    int myLen = myD.length;
    int oLen = oD.length;

    if (myLen >= oLen) {
      return false;
    }

    int len = myLen;
    int pos = -1;
    while (++pos < len) {
      if (myD[pos] != oD[pos]) {
        return false;
      }
    }

    return true;
  }

  public boolean isAncestorOrSelfOf(SirixDeweyID deweyID) {
    int[] myD = divisionValues;
    int[] oD = deweyID.divisionValues;
    int myLen = myD.length;
    int oLen = oD.length;

    if (myLen > oLen) {
      return false;
    }

    int len = myLen;
    int pos = -1;
    while (++pos < len) {
      if (myD[pos] != oD[pos]) {
        return false;
      }
    }

    return true;
  }

  public boolean isParentOf(SirixDeweyID deweyID) {
    int[] myD = divisionValues;
    int[] oD = deweyID.divisionValues;
    int myLen = myD.length;
    int oLen = oD.length;

    if ((myLen - oLen != -1) && ((myLen != oLen - 2) || (oD[oLen - 2] != 1))) {
      return false;
    }

    int len = myLen;
    int pos = -1;
    while (++pos < len) {
      if (myD[pos] != oD[pos]) {
        return false;
      }
    }

    return true;
  }

  public boolean isPrecedingSiblingOf(SirixDeweyID deweyID) {
    if (!isSiblingOf(deweyID)) {
      return false;
    }

    int myLen = divisionValues.length;
    int oLen = deweyID.divisionValues.length;
    int checkPos = Math.min(myLen - 1, oLen - 1);
    return ((divisionValues[checkPos] < deweyID.divisionValues[checkPos]));
  }

  public boolean isPrecedingOf(SirixDeweyID deweyID) {
    return ((compareTo(deweyID) < 0) && (!isAncestorOf(deweyID)));
  }

  public boolean isSiblingOf(SirixDeweyID deweyID) {
    if ((level == 0 || deweyID.level == 0) || (level != deweyID.level)) {
      return false;
    }

    final int[] myD = divisionValues;
    final int[] oD = deweyID.divisionValues;
    int myP = 0;
    int oP = 0;

    while ((myP < myD.length - 1) && (oP < oD.length - 1)) {
      if (myD[myP] == oD[oP]) {
        myP++;
        oP++;
      } else if (((myD[myP] % 2 == 0)) || (oD[oP] % 2 == 0)) {
        while (myD[myP] % 2 == 0)
          myP++;
        while (oD[oP] % 2 == 0)
          oP++;
        int rLenDiff = (myD.length - myP) - (oD.length - oP);
        return (rLenDiff == 0);
      } else {
        return false;
      }
    }

    return ((myD[myP] != 1) && (oD[oP] != 1) && (myD[myP] != oD[oP]));
  }

  public boolean isFollowingSiblingOf(SirixDeweyID deweyID) {
    if (!isSiblingOf(deweyID)) {
      return false;
    }

    int myLen = divisionValues.length;
    int oLen = deweyID.divisionValues.length;
    int checkPos = Math.min(myLen - 1, oLen - 1);
    return ((divisionValues[checkPos] > deweyID.divisionValues[checkPos]));
  }

  public boolean isFollowingOf(SirixDeweyID deweyID) {
    return (compareTo(deweyID) > 0) && (!isAncestorOf(deweyID)) && (!deweyID.isAncestorOf(this));
  }

  public boolean isChildOf(SirixDeweyID deweyID) {
    return deweyID.isParentOf(this);
  }

  public boolean isDescendantOf(SirixDeweyID deweyID) {
    return deweyID.isAncestorOf(this);
  }

  public boolean isDescendantOrSelfOf(SirixDeweyID deweyID) {
    return deweyID.isAncestorOrSelfOf(this);
  }

  public boolean isAttribute() {
    return ((level > 1) && (divisionValues.length > 2) && (divisionValues[divisionValues.length - 2]
        == SirixDeweyID.attributeRootDivisionValue));
  }

  public boolean isRecordValue() {
    return ((level > 1) && (divisionValues.length > 1) && (divisionValues[divisionValues.length - 1]
        == SirixDeweyID.recordValueRootDivisionValue));
  }

  public boolean isAttributeRoot() {
    return ((level > 1) && (divisionValues.length > 1) && (divisionValues[divisionValues.length - 1]
        == SirixDeweyID.attributeRootDivisionValue));
  }

  // ancestor or self semantics
  public SirixDeweyID getAncestor(int level) {
    if (this.level == level) {
      return this;
    }

    if (this.level < level) {
      return null;
    }

    int currDivision = 0;
    for (int i = 0; i < level; i++) {
      while (divisionValues[currDivision] % 2 == 0)
        currDivision++;
      currDivision++;
    }

    SirixDeweyID newID = new SirixDeweyID(Arrays.copyOf(divisionValues, currDivision), level);
    return newID;

  }

  /**
   * Like {@link #getAncestor(int)} but it checks in addition whether the ancestor has the given
   * DeweyID as prefix (or whether the ancestor is itself a prefix of the given DeweyID). If the
   * prefix condition is not satisfied, null is returned.
   *
   * @param level
   * @param requiredPrefix
   * @return
   */
  public SirixDeweyID getAncestor(int level, SirixDeweyID requiredPrefix) {
    if (this.level < level) {
      return null;
    }

    int currDivision = 0;
    for (int i = 0; i < level; i++) {
      while (this.divisionValues[currDivision] % 2 == 0) {
        if (currDivision < requiredPrefix.divisionValues.length
            && this.divisionValues[currDivision] != requiredPrefix.divisionValues[currDivision]) {
          return null;
        }
        currDivision++;
      }

      if (currDivision < requiredPrefix.divisionValues.length
          && this.divisionValues[currDivision] != requiredPrefix.divisionValues[currDivision]) {
        return null;
      }
      currDivision++;
    }

    if (this.level == level) {
      return this;
    } else {
      return new SirixDeweyID(Arrays.copyOf(divisionValues, currDivision), level);
    }
  }

  public SirixDeweyID[] getAncestors() {
    if (level == 0) {
      return null;
    }

    SirixDeweyID id = this;
    SirixDeweyID[] ancestors = new SirixDeweyID[level];

    for (int i = level; i > 0; i--) {
      ancestors[i - 1] = id.getParent();
      id = id.getParent();
    }
    return ancestors;
  }

  public SirixDeweyID[] getAncestors(SirixDeweyID lca) {
    if (!lca.isAncestorOf(this)) {
      return null;
    }

    SirixDeweyID id = this;
    SirixDeweyID[] ancestors = new SirixDeweyID[this.level - lca.getLevel() - 1];

    for (int i = ancestors.length; i > 0; i--) {
      ancestors[i - 1] = id.getParent();
      id = id.getParent();
    }
    return ancestors;
  }

  public boolean isLCA(SirixDeweyID id) {
    return (getLCA(id).compareTo(id) == 0);
  }

  public SirixDeweyID getLCA(SirixDeweyID id) {
    int common_length = 0;
    int length = Math.min(divisionValues.length, id.divisionValues.length);
    for (int i = 0; i < length; i++) {
      if (id.divisionValues[i] == divisionValues[i])
        common_length++;
      else
        break;
    }
    while (divisionValues[common_length - 1] % 2 == 0)
      common_length--;
    return new SirixDeweyID(Arrays.copyOf(divisionValues, common_length));
  }

  public int calcLCALevel(SirixDeweyID id) {
    int lcaLevel = 0;
    int a = divisionValues.length;
    int b = id.divisionValues.length;
    int maxPos = ((a <= b) ? a : b);

    for (int i = 0; i < maxPos; i++) {
      if (id.divisionValues[i] == divisionValues[i]) {
        if (divisionValues[i] % 2 != 0) {
          lcaLevel++;
        }
      } else {
        break;
      }
    }

    return lcaLevel;
  }

  public SirixDeweyID getParent() {
    if (level == 0) {
      return null;
    }

    int i = divisionValues.length - 2;
    while ((i >= 0) && (divisionValues[i] % 2 == 0))
      i--;

    SirixDeweyID parent = new SirixDeweyID(Arrays.copyOf(divisionValues, i + 1), level - 1);
    return parent;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(divisionValues);
  }

  public static SirixDeweyID newBetween(SirixDeweyID deweyID1, SirixDeweyID deweyID2)
  {
    // newBetween always returns ID of new node in same level!

    if ((deweyID1 == null) && (deweyID2 != null)) {
      // return previous sibling ID of deweyID2 if deweyID2 is first child
      int i = deweyID2.divisionValues.length - 2; // start at penultimate
      // position
      while ((i >= 0) && (deweyID2.divisionValues[i] % 2 == 0))
        i--; // scan to front while even divisions are located

      i++;
      // skip the 2s
      while (deweyID2.divisionValues[i] == 2 || deweyID2.divisionValues[i] == recordValueRootDivisionValue)
        i++;

      int divisions;
      int[] divisionValues;
      if ((deweyID2.divisionValues[i] % 2 == 1) && (deweyID2.divisionValues[i] > 3)) {
        // odd Division > 3, last division / 2
        divisions = deweyID2.getNumberOfDivisions();
        divisionValues = new int[divisions];
        for (int j = 0; j < divisions - 1; j++) {
          divisionValues[j] = deweyID2.divisionValues[j];
        }
        divisionValues[divisions - 1] = deweyID2.divisionValues[divisions - 1] / 2;
        // make sure last division is odd
        if (divisionValues[divisions - 1] % 2 == 0)
          divisionValues[divisions - 1]++;
      } else if (deweyID2.divisionValues[i] == 3) {
        // x.3 gets x.2.distanceToSibling+1
        divisions = deweyID2.getNumberOfDivisions() + 1;
        divisionValues = new int[divisions];
        for (int j = 0; j < divisions - 1; j++) {
          divisionValues[j] = deweyID2.divisionValues[j];
        }
        divisionValues[i] = 2;
        divisionValues[i + 1] = distanceToSibling + 1;
      } else { // even division > 2
        // current division /2
        divisions = i + 1;
        divisionValues = new int[divisions];
        for (int j = 0; j < divisions - 1; j++) {
          divisionValues[j] = deweyID2.divisionValues[j];
        }
        divisionValues[i] = deweyID2.divisionValues[i] / 2;
        // make sure last division is odd
        if (divisionValues[i] % 2 == 0)
          divisionValues[i]++;
      }

      SirixDeweyID newID = new SirixDeweyID(Arrays.copyOf(divisionValues, divisions), deweyID2.level);
      return newID;
    } else if ((deweyID1 != null) && (deweyID2 == null)) {
      int[] tmp = Arrays.copyOf(deweyID1.divisionValues, deweyID1.divisionValues.length);
      tmp[tmp.length - 1] += distanceToSibling;
      SirixDeweyID newID = new SirixDeweyID(tmp, deweyID1.level);
      return newID;
    } else // two IDs given
    {
      if (deweyID1.compareTo(deweyID2) >= 0)
        throw new SirixException("DeweyID [newBetween]: deweyID1 is greater or equal to deweyID2");
      if (deweyID1.getParent().compareTo(deweyID2.getParent()) != 0)
        throw new SirixException("DeweyID [newBetween]: deweyID1 and deweyID2 are no siblings");
      // return new deweyID between deweyID1 and deweyID2

      // first scan to first different divisions
      int i = 0;
      while (deweyID1.divisionValues[i] == deweyID2.divisionValues[i])
        i++;
      int divisions;
      int[] divisionValues;

      if (deweyID2.divisionValues[i] - deweyID1.divisionValues[i] > 2) {
        // ready, because odd division fits
        // between the two given IDs
        divisions = i + 1;
        divisionValues = new int[divisions];
        for (int j = 0; j < divisions - 1; j++) {
          divisionValues[j] = deweyID1.divisionValues[j];
        }

        divisionValues[divisions - 1] = deweyID1.divisionValues[divisions - 1]
            + (deweyID2.divisionValues[divisions - 1] - deweyID1.divisionValues[divisions - 1]) / 2;
        // take care that division is odd
        if ((divisionValues[divisions - 1] % 2) == 0)
          divisionValues[divisions - 1] -= 1;
      } else if (deweyID2.divisionValues[i] - deweyID1.divisionValues[i] == 2) {
        // only one division number fits between
        // perhaps an odd division fits in
        if (deweyID2.divisionValues[i] % 2 == 0) {
          // odd division fits in
          divisions = i + 1;
          divisionValues = new int[divisions];
          for (int j = 0; j < divisions - 1; j++) {
            divisionValues[j] = deweyID1.divisionValues[j];
          }
          divisionValues[divisions - 1] = deweyID1.divisionValues[divisions - 1] + 1;
        } else { // only even division fits in
          divisions = i + 2;
          divisionValues = new int[divisions];
          for (int j = 0; j < divisions - 1; j++) {
            divisionValues[j] = deweyID1.divisionValues[j];
          }
          divisionValues[divisions - 2] += 1;
          divisionValues[divisions - 1] = distanceToSibling + 1;
        }
      } else {
        // one deweyID is parsed to the end but still no new deweyID
        // between found
        // and no DeweyID fits between the two divisions(these cases are
        // handled with above
        // two possibilities
        if (deweyID1.divisionValues[i] % 2 == 1) { // deweyID1 complete
          i++;
          // overparse the 2
          while (deweyID2.divisionValues[i] == 2)
            i++;
          if (deweyID2.divisionValues[i] == 3) { // last division is 3
            // add 2.distanceToSibling+1
            divisions = i + 2;
            divisionValues = new int[divisions];
            for (int j = 0; j < divisions - 2; j++) {
              divisionValues[j] = deweyID2.divisionValues[j];
            }
            divisionValues[divisions - 2] = 2;
            divisionValues[divisions - 1] = distanceToSibling + 1;
          } else { // division >3
            divisions = i + 1;
            divisionValues = new int[divisions];
            for (int j = 0; j < divisions; j++) {
              divisionValues[j] = deweyID2.divisionValues[j];
            }
            divisionValues[divisions - 1] /= 2;
            // make sure division is odd
            if (divisionValues[divisions - 1] % 2 == 0)
              divisionValues[divisions - 1] += 1;
          }

        } else { // deweyID2 complete
          i++;
          divisions = i + 1;
          divisionValues = new int[divisions];
          for (int j = 0; j < divisions; j++) {
            divisionValues[j] = deweyID1.divisionValues[j];
          }
          if (deweyID1.divisionValues[i] % 2 == 1) { // odd
            // last division + distanceToSibling
            divisionValues[divisions - 1] += distanceToSibling;
          } else { // even
            divisionValues[divisions - 1] += distanceToSibling - 1;
            // lastdivision + (distanceToSibling - 1);
          }
        }
      }

      SirixDeweyID newID = new SirixDeweyID(divisionValues, deweyID1.level);
      return newID;
    }
  }

  public final static SirixDeweyID newRootID() {
    return new SirixDeweyID(new int[] { 1 }, 1);
  }

  public final SirixDeweyID getNewChildID() {
    return (level > 0) ? new SirixDeweyID(this, SirixDeweyID.distanceToSibling + 1) : new SirixDeweyID(this, 1);
  }

  public final SirixDeweyID getNewChildID(int division) {
    return new SirixDeweyID(this, division);
  }

  public final SirixDeweyID getNewAttributeID() {
    int[] childDivisions = Arrays.copyOf(divisionValues, divisionValues.length + 2);
    childDivisions[divisionValues.length] = SirixDeweyID.attributeRootDivisionValue;
    childDivisions[divisionValues.length + 1] = SirixDeweyID.distanceToSibling + 1;

    SirixDeweyID newID = new SirixDeweyID(childDivisions, level + 1);

    return newID;
  }

  public final SirixDeweyID getNewNamespaceID() {
    int[] childDivisions = Arrays.copyOf(divisionValues, divisionValues.length + 2);
    childDivisions[divisionValues.length] = SirixDeweyID.namespaceRootDivisionValue;
    childDivisions[divisionValues.length + 1] = SirixDeweyID.distanceToSibling + 1;

    SirixDeweyID newID = new SirixDeweyID(childDivisions, level + 1);

    return newID;
  }

  public final SirixDeweyID getNewRecordID() {
    int[] childDivisions = Arrays.copyOf(divisionValues, divisionValues.length + 2);
    childDivisions[divisionValues.length] = SirixDeweyID.recordValueRootDivisionValue;
    childDivisions[divisionValues.length + 1] = SirixDeweyID.distanceToSibling + 1;

    SirixDeweyID newID = new SirixDeweyID(childDivisions, level + 1);

    return newID;
  }

  public final SirixDeweyID getRecordValueRootID() {
    return new SirixDeweyID(this, SirixDeweyID.recordValueRootDivisionValue);
  }

  public final SirixDeweyID getAttributeRootID() {
    return new SirixDeweyID(this, SirixDeweyID.attributeRootDivisionValue);
  }

  public static String getDivisionLengths() {
    StringBuffer output = new StringBuffer();
    for (int i = 0; i < divisionLengthArray.length; i++) {
      output.append(divisionLengthArray[i] + " ");
    }
    return new String(output);
  }

  public static String getPrefixes() {
    StringBuffer output = new StringBuffer();
    for (int i = 0; i < bitStringAsBoolean.length; i++) {
      for (int j = 0; j < bitStringAsBoolean[i].length; j++) {
        output.append(bitStringAsBoolean[i][j] + " ");
      }
      output.append(";");
    }
    return new String(output);
  }

  public StringBuffer list() {
    StringBuffer output = new StringBuffer();
    output.append("" + this.toString());
    output.append("\t");
    // calculate needed bits for deweyID
    byte[] byteArray = this.toBytes();
    for (int i = 0; i < byteArray.length; i++) {
      output.append(byteArray[i] + "\t");
    }
    output.append("");

    int helpFindingBit;
    for (int bitIndex = 0; bitIndex < (8 * byteArray.length); bitIndex++) {

      switch (bitIndex % 8) {
        case 0:
          helpFindingBit = 128;
          break;
        case 1:
          helpFindingBit = 64;
          break;
        case 2:
          helpFindingBit = 32;
          break;
        case 3:
          helpFindingBit = 16;
          break;
        case 4:
          helpFindingBit = 8;
          break;
        case 5:
          helpFindingBit = 4;
          break;
        case 6:
          helpFindingBit = 2;
          break;
        default:
          helpFindingBit = 1;
          break;
      }

      if ((byteArray[bitIndex / 8] & helpFindingBit) == helpFindingBit) {
        // bit is set
      } else {
      }
    }

    return output;
  }

  public boolean equals(SirixDeweyID other) {
    return compareTo(other) == 0;
  }

  public boolean isRoot() {
    return level == 1;
  }

  public boolean isDocument() {
    return level == 0;
  }

  /**
   * Checks whether this DeweyID is a prefix of the other.
   *
   * @param other the other DeweyID
   * @return true if this DeweyID is a prefix of the other DeweyID
   */
  public boolean isPrefixOf(SirixDeweyID other) {

    if (other.divisionValues.length < this.divisionValues.length) {
      return false;
    }

    for (int i = 0; i < this.divisionValues.length; i++) {
      if (this.divisionValues[i] != other.divisionValues[i]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Like {@link #compareTo(SirixDeweyID)} but without checking the collection ID. Only the
   * divisions are considered.
   *
   * @param deweyID the other DeweyID
   * @return -1 if this DeweyID is less than the other, 0 if they are equal, and 1 if this DeweyID
   * is greater than the other
   */
  public int compareReduced(SirixDeweyID deweyID) {
    if (this == deweyID) {
      return 0;
    }

    int[] myD = this.divisionValues;
    int[] oD = deweyID.divisionValues;
    int myLen = myD.length;
    int oLen = oD.length;
    int len = ((myLen <= oLen) ? myLen : oLen);

    int pos = -1;
    while (++pos < len) {
      if (myD[pos] != oD[pos]) {
        return myD[pos] - oD[pos];
      }
    }

    return (myLen == oLen) ? 0 : (myLen < oLen) ? -1 : 1;
  }

  /**
   * Compares this DeweyID's parent with the given DeweyID (except for the collection ID).
   *
   * @param other the other DeweyID
   * @return a negative number if the parent is less than the other DeweyID, 0 if they are equal,
   * and a positive number if the parent is greater than the other DeweyID
   */
  public int compareParentTo(SirixDeweyID other) {
    int parentLength = this.divisionValues.length - 1;
    while (this.divisionValues[parentLength - 1] % 2 == 0) {
      parentLength--;
    }

    int upperBound = Math.min(parentLength, other.divisionValues.length);

    for (int i = 0; i < upperBound; i++) {
      if (this.divisionValues[i] != other.divisionValues[i]) {
        return (this.divisionValues[i] < other.divisionValues[i]) ? -1 : 1;
      }
    }

    return Integer.signum(parentLength - other.divisionValues.length);
  }

  /**
   * Checks whether this DeweyID is either a prefix or greater than the other DeweyID.
   *
   * @param other the other DeweyID
   * @return true if this DeweyID is a prefix or greater than the other DeweyID
   */
  public boolean isPrefixOrGreater(SirixDeweyID other) {
    int upperBound = (this.divisionValues.length <= other.divisionValues.length)
        ? this.divisionValues.length
        : other.divisionValues.length;

    for (int i = 0; i < upperBound; i++) {
      if (this.divisionValues[i] != other.divisionValues[i]) {
        return (this.divisionValues[i] > other.divisionValues[i]);
      }
    }

    return true;
  }

  /**
   * Checks whether this DeweyID appended by the extraDivision is either a prefix or greater than
   * the other DeweyID.
   *
   * @param other the other DeweyID
   * @return true if this DeweyID appended by the extraDivision is a prefix or greater than the
   * other DeweyID
   */
  public boolean isPrefixOrGreater(int extraDivision, SirixDeweyID other) {
    boolean isShorter = (this.divisionValues.length < other.divisionValues.length);
    int upperBound = (isShorter ? this.divisionValues.length : other.divisionValues.length);

    for (int i = 0; i < upperBound; i++) {
      if (this.divisionValues[i] != other.divisionValues[i]) {
        return (this.divisionValues[i] > other.divisionValues[i]);
      }
    }

    // check extra division
    if (isShorter) {
      if (extraDivision != other.divisionValues[upperBound]) {
        return (extraDivision > other.divisionValues[upperBound]);
      }
    }

    // at this point, one DeweyID is a prefix of the other one -> this
    // DeweyID is either a prefix of the other, or greater
    return true;
  }
}
