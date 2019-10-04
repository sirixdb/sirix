/**
 * Copyright (c) 2018, Sirix
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
package org.sirix.xquery.json;

import com.google.gson.stream.JsonReader;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.JsonStore;

import java.nio.file.Path;
import java.util.Set;

/**
 * Database store.
 *
 * @author Johannes Lichtenberger <lichtenberger.johannes@gmail.com>
 */
public interface JsonDBStore extends JsonStore, AutoCloseable {
  @Override
  JsonDBCollection lookup(String name);

  @Override
  JsonDBCollection create(String name);

  @Override
  JsonDBCollection create(String collName, Path path);

  @Override
  JsonDBCollection createFromPaths(String collName, Stream<Path> path);

  @Override
  JsonDBCollection create(String collName, String optResName, Path path);

  @Override
  JsonDBCollection create(String collName, String path);

  @Override
  JsonDBCollection create(String collName, String optResName, String json);

  JsonDBCollection create(String collName, String optResName, JsonReader json);

  JsonDBCollection create(String collName, Set<JsonReader> json);

  @Override
  JsonDBCollection createFromJsonStrings(String collName, Stream<Str> json);

  @Override
  void drop(String name);

  @Override
  void makeDir(String path);

  @Override
  void close();
}
