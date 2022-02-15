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
package org.sirix.io.bytepipe;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public enum ByteHandlerKind {
  DEFLATE_COMPRESSOR(DeflateCompressor.class) {
    @Override
    public ByteHandler deserialize(JsonReader reader) {
      return callDefaultConstructor(reader, DeflateCompressor.class.getName());
    }

    @Override
    public void serialize(ByteHandler byteHandler, JsonWriter writer) throws IOException {
      serializeDefaultConstructor(byteHandler, writer);
    }
  },

  SNAPPY_COMPRESSOR(SnappyCompressor.class) {
    @Override
    public ByteHandler deserialize(JsonReader reader) {
      return callDefaultConstructor(reader, SnappyCompressor.class.getName());
    }

    @Override
    public void serialize(ByteHandler byteHandler, JsonWriter writer) throws IOException {
      serializeDefaultConstructor(byteHandler, writer);
    }
  },

  ENCRYPTOR(Encryptor.class) {
    @Override
    public ByteHandler deserialize(JsonReader reader) {
      try {
        final Class<?> handlerClazz = Encryptor.class;
        final Constructor<?> handlerCons = handlerClazz.getConstructor(Path.class);
        final Path path = Paths.get(reader.nextString());
        return (ByteHandler) handlerCons.newInstance(path);
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
          | InvocationTargetException | IOException | NoSuchMethodException | SecurityException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void serialize(ByteHandler byteHandler, JsonWriter writer) {
      try {
        writer.beginObject();
        writer.name(byteHandler.getClass().getName());
        writer.value(((Encryptor) byteHandler).getResourcePath().toString());
        writer.endObject();
      } catch (IllegalArgumentException | IOException e) {
        throw new IllegalStateException(e);
      }
    }
  };

  public abstract ByteHandler deserialize(JsonReader reader) throws IOException;

  public abstract void serialize(ByteHandler byteHandler, JsonWriter writer) throws IOException;

  /** Mapping of class -> byte handler kind. */
  private static final Map<Class<? extends ByteHandler>, ByteHandlerKind> INSTANCEFORCLASS =
      new HashMap<>();

  static {
    for (final ByteHandlerKind byteHandler : values()) {
      INSTANCEFORCLASS.put(byteHandler.mClass, byteHandler);
    }
  }

  /** Class. */
  private final Class<? extends ByteHandler> mClass;

  /**
   * Constructor.
   *
   * @param clazz class
   */
  ByteHandlerKind(final Class<? extends ByteHandler> clazz) {
    mClass = clazz;
  }

  private static void serializeDefaultConstructor(ByteHandler byteHandler, JsonWriter writer)
      throws IOException {
    writer.beginObject();
    writer.name(byteHandler.getClass().getName());
    writer.nullValue();
    writer.endObject();
  }

  private static ByteHandler callDefaultConstructor(JsonReader reader, String className) {
    try {
      reader.nextNull();
      final Class<?> handlerClazz = Class.forName(className);
      final Constructor<?> handlerCons = handlerClazz.getConstructors()[0];
      return (ByteHandler) handlerCons.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Public method to get the related byte handler based on the class.
   *
   * @param clazz the class for the page
   * @return the related page
   */
  public static @NonNull ByteHandlerKind getKind(final Class<? extends ByteHandler> clazz) {
    final ByteHandlerKind byteHandler = INSTANCEFORCLASS.get(clazz);
    if (byteHandler == null) {
      throw new IllegalStateException();
    }
    return byteHandler;
  }
}
