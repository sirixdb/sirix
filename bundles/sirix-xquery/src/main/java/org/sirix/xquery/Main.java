/*
 * [New BSD License]
 * Copyright (c) 2011-2012, Brackit Project Team <info@brackit.org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Brackit Project Team nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.xquery;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.util.io.URIHandler;
import org.brackit.xquery.xdm.node.Node;
import org.brackit.xquery.xdm.node.TemporalNodeCollection;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.xquery.json.BasicJsonDBStore;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.node.BasicXmlDBStore;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * @author Sebastian Baechle
 * @author Johannes Lichtenberger
 */
public final class Main {

  private static class Config {
    final Map<String, String> options = new HashMap<String, String>();

    boolean isSet(final String option) {
      return options.containsKey(option);
    }

    String getValue(final String option) {
      return options.get(option);
    }

    void setOption(final String option, final String value) {
      options.put(option, value);
    }
  }

  private static class Option {
    final String key;
    final String desc;
    final boolean hasValue;

    Option(final String key, final String desc, final boolean hasValue) {
      this.key = key;
      this.desc = desc;
      this.hasValue = hasValue;
    }
  }

  private static List<Option> options = new ArrayList<>();

  static {
    options.add(new Option("-q", "query file [use '-' for stdin (default)]", true));
    options.add(new Option("-fType", "default document type", true));
    options.add(new Option("-f", "default document", true));
    options.add(new Option("-p", "pretty print", false));
  }

  public static void main(final String[] args) {
    try {
      final Config config = parseParams(args);
      try (final BasicXmlDBStore nodeStore = BasicXmlDBStore.newBuilder().build();
           final BasicJsonDBStore jsonStore = BasicJsonDBStore.newBuilder().build()) {
        final QueryContext ctx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(nodeStore,
                                                                                                    jsonStore,
                                                                                                    SirixQueryContext.CommitStrategy.AUTO);

        final String file = config.getValue("-f");
        final String fileType = config.getValue("-fType");

        if (file != null) {
          if ("json".equals(fileType)) {
            try (final var reader = JsonShredder.createFileReader(Path.of(file))) {
              final JsonDBCollection coll = jsonStore.create(file, Set.of(reader));
              final JsonDBItem doc = coll.getDocument();
              ctx.setContextItem(doc);
            }
          } else {
            final URI uri = new URI(file);
            final InputStream in = URIHandler.getInputStream(uri);
            try {
              final SubtreeParser parser = new DocumentParser(in);
              final String name = uri.toURL().getFile();

              final TemporalNodeCollection<?> coll = nodeStore.create(name, parser);
              final Node<?> doc = coll.getDocument();
              ctx.setContextItem(doc);
            } finally {
              in.close();
            }
          }
        }

        final String query;

        if (((config.isSet("-q")) && (!"-".equals(config.getValue("-q"))))) {
          query = readFile(config.getValue("-q"));
        } else {
          query = readStringFromScanner(System.in);
        }

        final XQuery xq = new XQuery(SirixCompileChain.createWithNodeAndJsonStore(nodeStore, jsonStore), query);
        if (config.isSet("-p")) {
          xq.prettyPrint();
        }
        xq.serialize(ctx, System.out);
      }
    } catch (final QueryException e) {
      System.out.println("Error: " + e.getMessage());
      System.exit(-2);
    } catch (final IOException e) {
      System.out.println("I/O Error: " + e.getMessage());
      System.exit(-3);
    } catch (final Throwable e) {
      System.out.println("Error: " + e.getMessage());
      System.exit(-4);
    }
  }

  private static String readStringFromScanner(final InputStream in) {
    try (final Scanner scanner = new Scanner(in)) {
      final StringBuilder strbuf = new StringBuilder();

      while (scanner.hasNextLine()) {
        final String line = scanner.nextLine();

        if (line.trim().isEmpty())
          break;

        strbuf.append(line);
      }

      return strbuf.toString();
    }
  }

  private static Config parseParams(final String[] args) throws Exception {
    final Config config = new Config();
    for (int i = 0; i < args.length; i++) {
      boolean valid = false;
      final String s = args[i];
      for (final Option o : options) {
        if (o.key.equals(s)) {
          final String val = (o.hasValue) ? args[++i] : null;
          config.setOption(o.key, val);
          valid = true;
          break;
        }
      }
      if (!valid) {
        printUsage();
        throw new Exception("Invalid parameter: " + s);
      }
    }
    return config;
  }

  private static String readFile(final String file) throws IOException {
    return Files.readString(Path.of(file));
  }

  private static void printUsage() {
    System.out.println("No query provided");
    System.out.println(String.format("Usage: java %s [options]", Main.class.getName()));
    System.out.println("Options:");
    for (final Option o : options) {
      System.out.print(" ");
      System.out.print(o.key);
      if (o.hasValue) {
        System.out.print(" <param>\t");
      } else {
        System.out.print("\t\t");
      }
      System.out.print("- ");
      System.out.println(o.desc);
    }
    System.exit(-1);
  }
}
