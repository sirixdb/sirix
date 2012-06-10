/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.treetank.service.xml.xpath.xmark;

import java.lang.reflect.Field;

import org.treetank.exception.TTIOException;

/**
 * This class defines all XMark queries by factor and its corresponding results.
 * 
 * @author Patrick Lang, University of Konstanz
 */
public class XMarkBenchQueries {

  /*
   * Queries and expected results for factor 0.01; XML file with size 1MB
   */
  private final String Q1_Fac001 = "for $b in /site/people/person[@id=\"person0\"] return $b/name/text()";
  private final String R1_Fac001 = "Klemens Pelz";

  private final String Q2_Fac001 = "";
  private final String R2_Fac001 = "";

  private final String Q3_Fac001 = "";
  private final String R3_Fac001 = "";

  private final String Q4_Fac001 = "";
  private final String R4_Fac001 = "";

  private final String Q5_Fac001 =
    "fn:count(for $i in /site/closed_auctions/closed_auction[price/text() >= 40] return $i/price)";
  private final String R5_Fac001 = "75";

  private final String Q6_Fac001 = "for $b in //site/regions return fn:count($b//item)";
  private final String R6_Fac001 = "217";

  private final String Q7_Fac001 = "for $p in /site return fn:count($p//description) + "
    + "fn:count($p//annotation) + fn:count($p//emailaddress)";
  private final String R7_Fac001 = "916.0";

  private final String Q8_Fac001 = "";
  private final String R8_Fac001 = "";

  private final String Q9_Fac001 = "";
  private final String R9_Fac001 = "";

  private final String Q10_Fac001 = "";
  private final String R10_Fac001 = "";

  private final String Q11_Fac001 = "";
  private final String R11_Fac001 = "";

  private final String Q12_Fac001 = "";
  private final String R12_Fac001 = "";

  private final String Q13_Fac001 = "";
  private final String R13_Fac001 = "";

  private final String Q14_Fac001 = "";
  private final String R14_Fac001 = "";

  private final String Q15_Fac001 = "";
  private final String R15_Fac001 = "";

  private final String Q16_Fac001 = "";
  private final String R16_Fac001 = "";

  private final String Q17_Fac001 = "";
  private final String R17_Fac001 = "";

  private final String Q18_Fac001 = "";
  private final String R18_Fac001 = "";

  private final String Q19_Fac001 = "";
  private final String R19_Fac001 = "";

  private final String Q20_Fac001 = "";
  private final String R20_Fac001 = "";

  // own queries
  private final String Q21_Fac001 = "/site/regions/*/item[@id=\"item0\"]/description//keyword/text()";
  private final String R21_Fac001 = "officer embrace such fears distinction attires";

  // private final String Q22_Fac001 =
  // "/site/open_auctions/open_auction[@id=\"open_auction0\"]/bidder/personref[@person=\"person175\"]";
  // private final String R22_Fac001 = "<personref person=\"person175\"/>";
  //
  // private final String Q23_Fac001 =
  // "/site/people/person[@id=\"person3\"][address and (phone or homepage)]/name";
  // private final String R23_Fac001 = "<name>Mehrdad Suermann</name>";

  /*
   * Queries and expected results for factor 0.1; XML file with size 10MB
   */
  private final String Q1_Fac01 = "for $b in /site/people/person[@id=\"person0\"] return $b/name/text()";
  private final String R1_Fac01 = "Krishna Merle";

  private final String Q2_Fac01 = "";
  private final String R2_Fac01 = "";

  private final String Q3_Fac01 = "";
  private final String R3_Fac01 = "";

  private final String Q4_Fac01 = "";
  private final String R4_Fac01 = "";

  private final String Q5_Fac01 =
    "fn:count(for $i in /site/closed_auctions/closed_auction[price/text() >= 40] return $i/price)";
  private final String R5_Fac01 = "670";

  private final String Q6_Fac01 = "for $b in //site/regions return fn:count($b//item)";
  private final String R6_Fac01 = "2175";

  private final String Q7_Fac01 = "for $p in /site return fn:count($p//description) + "
    + "fn:count($p//annotation) + fn:count($p//emailaddress)";
  private final String R7_Fac01 = "9175.0";

  private final String Q8_Fac01 = "";
  private final String R8_Fac01 = "";

  private final String Q9_Fac01 = "";
  private final String R9_Fac01 = "";

  private final String Q10_Fac01 = "";
  private final String R10_Fac01 = "";

  private final String Q11_Fac01 = "";
  private final String R11_Fac01 = "";

  private final String Q12_Fac01 = "";
  private final String R12_Fac01 = "";

  private final String Q13_Fac01 = "";
  private final String R13_Fac01 = "";

  private final String Q14_Fac01 = "";
  private final String R14_Fac01 = "";

  private final String Q15_Fac01 = "";
  private final String R15_Fac01 = "";

  private final String Q16_Fac01 = "";
  private final String R16_Fac01 = "";

  private final String Q17_Fac01 = "";
  private final String R17_Fac01 = "";

  private final String Q18_Fac01 = "";
  private final String R18_Fac01 = "";

  private final String Q19_Fac01 = "";
  private final String R19_Fac01 = "";

  private final String Q20_Fac01 = "";
  private final String R20_Fac01 = "";

  // own queries
  private final String Q21_Fac01 = "/site/regions/*/item[@id=\"item0\"]/description//keyword/text()";
  private final String R21_Fac01 = "officer embrace such fears distinction attires";

  /*
   * Queries for factor 1.0; XML file with size 100 MB
   */
  private final String Q1_Fac1 = "for $b in /site/people/person[@id=\"person0\"] return $b/name/text()";
  private final String R1_Fac1 = "Kasidit Treweek";

  private final String Q2_Fac1 = "";
  private final String R2_Fac1 = "";

  private final String Q3_Fac1 = "";
  private final String R3_Fac1 = "";

  private final String Q4_Fac1 = "";
  private final String R4_Fac1 = "";

  private final String Q5_Fac1 =
    "fn:count(for $i in /site/closed_auctions/closed_auction[price/text() >= 40] return $i/price)";
  private final String R5_Fac1 = "6539";

  private final String Q6_Fac1 = "for $b in //site/regions return fn:count($b//item)";
  private final String R6_Fac1 = "21750";

  private final String Q7_Fac1 = "for $p in /site return fn:count($p//description) + "
    + "fn:count($p//annotation) + fn:count($p//emailaddress)";
  private final String R7_Fac1 = "91750.0";

  private final String Q8_Fac1 = "";
  private final String R8_Fac1 = "";

  private final String Q9_Fac1 = "";
  private final String R9_Fac1 = "";

  private final String Q10_Fac1 = "";
  private final String R10_Fac1 = "";

  private final String Q11_Fac1 = "";
  private final String R11_Fac1 = "";

  private final String Q12_Fac1 = "";
  private final String R12_Fac1 = "";

  private final String Q13_Fac1 = "";
  private final String R13_Fac1 = "";

  private final String Q14_Fac1 = "";
  private final String R14_Fac1 = "";

  private final String Q15_Fac1 = "";
  private final String R15_Fac1 = "";

  private final String Q16_Fac1 = "";
  private final String R16_Fac1 = "";

  private final String Q17_Fac1 = "";
  private final String R17_Fac1 = "";

  private final String Q18_Fac1 = "";
  private final String R18_Fac1 = "";

  private final String Q19_Fac1 = "";
  private final String R19_Fac1 = "";

  private final String Q20_Fac1 = "";
  private final String R20_Fac1 = "";

  // own queries
  private final String Q21_Fac1 = "/site/regions/*/item[@id=\"item0\"]/description//keyword/text()";
  private final String R21_Fac1 = "officer embrace such fears distinction attires";

  /*
   * Queries for factor 10.0; XML file with size 1000 MB
   */
  private final String Q1_Fac10 = "for $b in /site/people/person[@id=\"person0\"] return $b/name/text()";
  private final String R1_Fac10 = "Waldo Birch";

  private final String Q2_Fac10 = "";
  private final String R2_Fac10 = "";

  private final String Q3_Fac10 = "";
  private final String R3_Fac10 = "";

  private final String Q4_Fac10 = "";
  private final String R4_Fac10 = "";

  private final String Q5_Fac10 =
    "fn:count(for $i in /site/closed_auctions/closed_auction[price/text() >= 40] return $i/price)";
  private final String R5_Fac10 = "65851";

  private final String Q6_Fac10 = "for $b in //site/regions return fn:count($b//item)";
  private final String R6_Fac10 = "217500";

  private final String Q7_Fac10 = "for $p in /site return fn:count($p//description) + "
    + "fn:count($p//annotation) + fn:count($p//emailaddress)";
  private final String R7_Fac10 = "917500";

  private final String Q8_Fac10 = "";
  private final String R8_Fac10 = "";

  private final String Q9_Fac10 = "";
  private final String R9_Fac10 = "";

  private final String Q10_Fac10 = "";
  private final String R10_Fac10 = "";

  private final String Q11_Fac10 = "";
  private final String R11_Fac10 = "";

  private final String Q12_Fac10 = "";
  private final String R12_Fac10 = "";

  private final String Q13_Fac10 = "";
  private final String R13_Fac10 = "";

  private final String Q14_Fac10 = "";
  private final String R14_Fac10 = "";

  private final String Q15_Fac10 = "";
  private final String R15_Fac10 = "";

  private final String Q16_Fac10 = "";
  private final String R16_Fac10 = "";

  private final String Q17_Fac10 = "";
  private final String R17_Fac10 = "";

  private final String Q18_Fac10 = "";
  private final String R18_Fac10 = "";

  private final String Q19_Fac10 = "";
  private final String R19_Fac10 = "";

  private final String Q20_Fac10 = "";
  private final String R20_Fac10 = "";

  /**
   * Return the query by query number and factor
   * 
   * @param queryNr
   *          XMark query number.
   * @param factor
   *          Factor of XML size.
   * @return query Query by number and factor.
   */
  public String getQuery(final int queryNr, final String factor) {
    final StringBuilder sb = new StringBuilder();
    sb.append("Q");
    sb.append(Integer.toString(queryNr));
    sb.append("_Fac");
    String fac = null;
    if (factor.equals("0.01")) {
      fac = "001";
    } else if (factor.equals("0.1")) {
      fac = "01";
    } else if (factor.equals("1.0")) {
      fac = "1";
    } else if (factor.equals("10.0")) {
      fac = "10";
    } else {
      try {
        throw new TTIOException("XMark Benchmarking query factor does not exist!");
      } catch (final TTIOException mExp) {
        mExp.printStackTrace();
      }
    }
    sb.append(fac);

    String queryValue = null;
    try {
      final Field privateStringField = XMarkBenchQueries.class.getDeclaredField(sb.toString());
      privateStringField.setAccessible(true);
      queryValue = (String)privateStringField.get(this);

    } catch (final Exception mExp) {
      mExp.printStackTrace();
    }
    return queryValue;
  }

  /**
   * Return the result by query number and factor
   * 
   * @param queryNr
   *          XMark query number.
   * @param factor
   *          Factor of XML size.
   * @return result Query by number and factor.
   */
  public String getResult(final int queryNr, final String factor) {
    final StringBuilder sb = new StringBuilder();
    sb.append("R");
    sb.append(Integer.toString(queryNr));
    sb.append("_Fac");
    String fac = null;
    if (factor.equals("0.01")) {
      fac = "001";
    } else if (factor.equals("0.1")) {
      fac = "01";
    } else if (factor.equals("1.0")) {
      fac = "1";
    } else if (factor.equals("10.0")) {
      fac = "10";
    } else {
      try {
        throw new TTIOException("XMark Benchmarking result factor does not exist!");
      } catch (final TTIOException mExp) {
        mExp.printStackTrace();
      }
    }
    sb.append(fac);

    String resultValue = null;
    try {
      final Field privateStringField = XMarkBenchQueries.class.getDeclaredField(sb.toString());
      privateStringField.setAccessible(true);
      resultValue = (String)privateStringField.get(this);

    } catch (final Exception mExp) {
      mExp.printStackTrace();
    }
    return resultValue;

  }

}
