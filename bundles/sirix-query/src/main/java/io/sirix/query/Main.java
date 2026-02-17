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
import java.net.URISyntaxException;
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
  private static final String USER_HOME = System.getProperty("user.home");
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  private static final List<Option> options = initializeOptions();

  private Main() {
    // Prevent instantiation
  }

  public static void main(final String[] args) {
    try {
      Config config = parseParams(args);
      try (BasicXmlDBStore nodeStore = BasicXmlDBStore.newBuilder().build();
          BasicJsonDBStore jsonStore = BasicJsonDBStore.newBuilder().build()) {
        QueryContext ctx = createQueryContext(nodeStore, jsonStore, config);

        CompileChain compileChain = SirixCompileChain.createWithNodeAndJsonStore(nodeStore, jsonStore);
        executeUserQueries(config, compileChain, ctx);
      }
    } catch (Exception e) {
      handleException(e);
    }
  }

  private static QueryContext createQueryContext(BasicXmlDBStore nodeStore, BasicJsonDBStore jsonStore, Config config)
      throws IOException, QueryException, URISyntaxException {
    QueryContext ctx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(nodeStore, jsonStore,
        SirixQueryContext.CommitStrategy.AUTO);
    initializeContextWithFile(config, ctx, nodeStore, jsonStore);
    return ctx;
  }

  private static void initializeContextWithFile(Config config, QueryContext ctx, BasicXmlDBStore nodeStore,
      BasicJsonDBStore jsonStore) throws IOException, QueryException, URISyntaxException {
    String file = config.getValue("-f");
    String fileType = config.getValue("-f");

    if (file != null) {
      if ("json".equals(fileType)) {
        initializeJsonContext(file, ctx, jsonStore);
      } else {
        initializeXmlContext(file, ctx, nodeStore);
      }
    }
  }

  private static void initializeJsonContext(String file, QueryContext ctx, BasicJsonDBStore jsonStore)
      throws IOException, QueryException {
    try (var reader = JsonShredder.createFileReader(Path.of(file))) {
      JsonDBCollection coll = jsonStore.create(file, Set.of(reader));
      JsonDBItem doc = coll.getDocument();
      ctx.setContextItem(doc);
    }
  }

  private static void initializeXmlContext(String file, QueryContext ctx, BasicXmlDBStore nodeStore)
      throws IOException, QueryException, URISyntaxException {
    URI uri = new URI(file);
    try (InputStream in = URIHandler.getInputStream(uri)) {
      NodeSubtreeParser parser = new DocumentParser(in);
      String name = uri.toURL().getFile();
      TemporalNodeCollection<?> coll = nodeStore.create(name, parser);
      Node<?> doc = coll.getDocument();
      ctx.setContextItem(doc);
    }
  }

  private static void executeUserQueries(Config config, CompileChain compileChain, QueryContext ctx)
      throws IOException {
    if (config.isSet("-qf") && !"-".equals(config.getValue("-qf"))) {
      executeFileQuery(config, compileChain, ctx);
    } else if (config.isSet("-q")) {
      executeQuery(config, compileChain, ctx, config.getValue("-q"));
    } else if (config.isSet("-iq")) {
      executeInteractiveQuery(config, compileChain, ctx);
    } else if (config.isSet("-iqf")) {
      executeFileInteractiveQuery(config, compileChain, ctx);
    } else {
      executeQuery(config, compileChain, ctx, readString());
    }
  }

  private static void executeFileQuery(Config config, CompileChain compileChain, QueryContext ctx) throws IOException {
    String query = readFile(config.getValue("-qf"));
    executeQuery(config, compileChain, ctx, query);
  }

  private static void executeQuery(Config config, CompileChain compileChain, QueryContext ctx, String query) {
    if (query == null || query.trim().isEmpty()) {
      System.err.println("Error: Query cannot be empty.");
      return;
    }

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

  private static void executeInteractiveQuery(Config config, CompileChain compileChain, QueryContext ctx)
      throws IOException {
    Terminal terminal = TerminalBuilder.builder().system(true).build();
    LineReader lineReader = LineReaderBuilder.builder()
                                             .terminal(terminal)
                                             .variable(LineReader.SECONDARY_PROMPT_PATTERN, "%M%P > ")
                                             .variable(LineReader.INDENTATION, 2)
                                             .variable(LineReader.LIST_MAX, 100)
                                             .variable(LineReader.HISTORY_FILE, LOCATION.resolve("history"))
                                             .build();

    while (true) {
      System.out.println("Enter query string (terminate with Control-D):");
      String query = readStringFromScannerWithEndMark(lineReader);
      if (query == null)
        break;

      try {
        executeQuery(config, compileChain, ctx, query);
      } catch (QueryException e) {
        handleQueryException(config, e);
      }
    }
  }

  private static void executeFileInteractiveQuery(Config config, CompileChain compileChain, QueryContext ctx)
      throws IOException {
    while (true) {
      System.out.println("Enter query string (terminate with Control-D):");
      String query = readFile(config.getValue("-iqf"));
      if (query == null)
        break;

      try {
        executeQuery(config, compileChain, ctx, query);
      } catch (QueryException e) {
        handleQueryException(config, e);
      }
    }
  }

  private static void handleQueryException(Config config, QueryException e) {
    System.err.println("Error: " + e.getMessage());
    if (config.isSet("-d")) {
      e.printStackTrace();
    }
  }

  private static void handleException(Exception e) {
    if (e instanceof QueryException) {
      System.err.println("Error: " + e.getMessage());
    } else if (e instanceof IOException) {
      System.err.println("I/O Error: " + e.getMessage());
    } else {
      System.err.println("Error: " + e.getMessage());
    }

    System.exit(-1);
  }

  private static String readStringFromScannerWithEndMark(LineReader lineReader) {
    StringBuilder strbuf = new StringBuilder();

    for (int i = 0;; i++) {
      String line = lineReader.readLine("sirix > ");
      if (line == null || line.isEmpty())
        break;
      if (i != 0)
        strbuf.append(System.lineSeparator());
      strbuf.append(line);
    }

    return strbuf.isEmpty()
        ? null
        : strbuf.toString();
  }


  // private static String readStringFromScannerWithEndMark() {
  // final Scanner scanner = new Scanner(System.in);
  // final StringBuilder strbuf = new StringBuilder();
  //
  // for (int i = 0; scanner.hasNextLine(); i++) {
  // final String line = scanner.nextLine();
  //
  // if (line.isEmpty())
  // break;
  //
  // if (i != 0) {
  // strbuf.append(System.lineSeparator());
  // }
  // strbuf.append(line);
  // }
  //
  // return strbuf.isEmpty() ? null : strbuf.toString();
  // }


  private static String readString() throws IOException {
    int r;
    ByteArrayOutputStream payload = new ByteArrayOutputStream();

    while ((r = System.in.read()) != -1) {
      payload.write(r);
    }

    return payload.toString(StandardCharsets.UTF_8);
  }

  private static Config parseParams(String[] args) throws Exception {
    Config config = new Config();

    for (int i = 0; i < args.length; i++) {
      String s = args[i];
      Option option = findOption(s);
      if (option != null) {
        String val = option.hasValue
            ? args[++i]
            : null;
        config.setOption(option.key, val);
      } else {
        printUsage();
        throw new Exception("Invalid parameter: " + s);
      }
    }

    return config;
  }

  private static Option findOption(String key) {
    return options.stream().filter(o -> o.key.equals(key)).findFirst().orElse(null);
  }

  private static String readFile(String file) throws IOException {
    return Files.readString(Path.of(file));
  }

  private static void printUsage() {
    System.out.println("No query provided");
    System.out.printf("Usage: java %s [options]%n", Main.class.getName());
    System.out.println("Options:");

    for (Option o : options) {
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

  private static List<Option> initializeOptions() {
    List<Option> opts = new ArrayList<>();
    opts.add(new Option("-qf", "query file [use '-' for stdin (default)]", true));
    opts.add(new Option("-q", "query string", true));
    opts.add(new Option("-iqf", "query files [use '-' for stdin (default)]", false));
    opts.add(new Option("-iq", "query strings", false));
    opts.add(new Option("-fType", "default document type", true));
    opts.add(new Option("-f", "default document", true));
    opts.add(new Option("-p", "pretty print", false));
    opts.add(new Option("-d", "debug", false));
    return opts;
  }

  private static class Config {
    final Map<String, String> options = new HashMap<>();

    boolean isSet(String option) {
      return options.containsKey(option);
    }

    String getValue(String option) {
      return options.get(option);
    }

    void setOption(String option, String value) {
      options.put(option, value);
    }
  }

  private static class Option {
    final String key;
    final String desc;
    final boolean hasValue;

    Option(String key, String desc, boolean hasValue) {
      this.key = key;
      this.desc = desc;
      this.hasValue = hasValue;
    }
  }
}
