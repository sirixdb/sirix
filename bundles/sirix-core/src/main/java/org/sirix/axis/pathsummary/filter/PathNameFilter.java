/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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

package org.sirix.axis.pathsummary.filter;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.index.path.summary.PathNode;
import org.sirix.index.path.summary.PathSummaryReader;

import java.util.function.Predicate;

/**
 * Filters path nodes based on a name.
 * @param name the name to filter
 * @param pathSummaryReader the reader (in case it has to fetch the name at first)
 *
 * @author Johannes Lichtenberger
 */
public record PathNameFilter(@NonNull String name, @NonNull PathSummaryReader pathSummaryReader)
    implements Predicate<PathNode> {
  @Override
  public boolean test(PathNode pathNode) {
    var qNm = pathNode.getName();
    if (qNm == null) {
      var nodeKey = pathSummaryReader.getNodeKey();
      pathSummaryReader.moveTo(pathNode.getNodeKey());
      qNm = pathSummaryReader.getName();
      pathSummaryReader.moveTo(nodeKey);
    }
    return name.equals(qNm.toString());
  }
}
