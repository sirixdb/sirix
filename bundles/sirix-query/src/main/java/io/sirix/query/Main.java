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
package io.sirix.query;

import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.node.BasicXmlDBStore;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Query;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.jdm.node.TemporalNodeCollection;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.brackit.query.util.io.URIHandler;
import io.sirix.service.json.shredder.JsonShredder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * @author Sebastian Baechle
 * @author Johannes Lichtenberger
 */
public final class Main {

  /**
   * User home directory.
   */
  private static final String USER_HOME = System.getProperty("user.home");

  /**
   * Storage for databases: Sirix data in home directory.
   */
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  private static class Config {
    final Map<String, String> options = new HashMap<>();

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

  private static final List<Option> options = new ArrayList<>();

  static {
    options.add(new Option("-qf", "query file [use '-' for stdin (default)]", true));
    options.add(new Option("-q", "query string", true));
    options.add(new Option("-iqf", "query files [use '-' for stdin (default)]", false));
    options.add(new Option("-iq", "query strings", false));
    options.add(new Option("-fType", "default document type", true));
    options.add(new Option("-f", "default document", true));
    options.add(new Option("-p", "pretty print", false));
    options.add(new Option("-d", "debug", false));
  }

  public static void main(final String[] args) {
    Config config = null;

    try {
      config = parseParams(args);
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
            try (InputStream in = URIHandler.getInputStream(uri)) {
              final NodeSubtreeParser parser = new DocumentParser(in);
              final String name = uri.toURL().getFile();

              final TemporalNodeCollection<?> coll = nodeStore.create(name, parser);
              final Node<?> doc = coll.getDocument();
              ctx.setContextItem(doc);
            }
          }
        }

        final var compileChain = SirixCompileChain.createWithNodeAndJsonStore(nodeStore, jsonStore);

        String query;

        if (config.isSet("-qf") && !"-".equals(config.getValue("-qf"))) {
          query = readFile(config.getValue("-qf"));
          executeQuery(config, compileChain, ctx, query);
        } else if (config.isSet("-q")) {
          query = config.getValue("-q");
          executeQuery(config, compileChain, ctx, query);
        } else if (config.isSet("-iq")) {
          final Terminal terminal = TerminalBuilder.builder().system(true).build();
          final LineReader lineReader = LineReaderBuilder.builder()
                                                         .terminal(terminal)
                                                         .variable(LineReader.SECONDARY_PROMPT_PATTERN, "%M%P > ")
                                                         .variable(LineReader.INDENTATION, 2)
                                                         .variable(LineReader.LIST_MAX, 100)
                                                         .variable(LineReader.HISTORY_FILE, LOCATION.resolve("history"))
                                                         .build();

          while (true) {
            System.out.println("Enter query string (terminate with Control-D):");
            query = readStringFromScannerWithEndMark(lineReader);
            if (query == null) {
              break;
            }
            try {
              executeQuery(config, compileChain, ctx, query);
            } catch (final QueryException e) {
              System.err.println(STR."Error: \{e.getMessage()}");
              if (config.isSet("-d")) {
                e.printStackTrace();
              }
            }
          }
        } else if (config.isSet("-iqf")) {
          while (true) {
            System.out.println("Enter query string (terminate with Control-D:");
            query = readFile(config.getValue("-iqf"));
            if (query == null) {
              break;
            }
            try {
              executeQuery(config, compileChain, ctx, query);
            } catch (final QueryException e) {
              System.err.println(STR."Error: \{e.getMessage()}");
              if (config.isSet("-d")) {
                e.printStackTrace();
              }
            }
          }
        } else {
          query = readString();
          executeQuery(config, compileChain, ctx, query);
        }
      }
    } catch (final QueryException e) {
      System.err.println(STR."Error: \{e.getMessage()}");
      if (config.isSet("-d")) {
        e.printStackTrace();
      }
      System.exit(-2);
    } catch (final IOException e) {
      System.err.println(STR."I/O Error: \{e.getMessage()}");
      if (config.isSet("-d")) {
        e.printStackTrace();
      }
      System.exit(-3);
    } catch (final Throwable e) {
      System.err.println(STR."Error: \{e.getMessage()}");
      if (config.isSet("-d")) {
        e.printStackTrace();
      }
      System.exit(-4);
    }
  }

  private static void executeQuery(Config config, CompileChain compileChain, QueryContext ctx, String query) {
    Query xq = new Query(compileChain, query);
    if (config.isSet("-p")) {
      xq.prettyPrint();
    }
    System.out.println();
    System.out.println("Query result");
    xq.serialize(ctx, System.out);
    System.out.println();
    System.out.println();
  }

  private static String readStringFromScannerWithEndMark(final LineReader lineReader) {
    final StringBuilder strbuf = new StringBuilder();

    for (int i = 0; ; i++) {
      final String line = lineReader.readLine("sirix > ");

      if (line == null)
        break;

      if (line.isEmpty())
        break;

      if (i != 0) {
        strbuf.append(System.lineSeparator());
      }
      strbuf.append(line);
    }

    return strbuf.isEmpty() ? null : strbuf.toString();
  }

  private static String readString() throws IOException {
    int r;
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    while ((r = System.in.read()) != -1) {
      payload.write(r);
    }
    return payload.toString(StandardCharsets.UTF_8);
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
    System.out.printf("Usage: java %s [options]%n", Main.class.getName());
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
