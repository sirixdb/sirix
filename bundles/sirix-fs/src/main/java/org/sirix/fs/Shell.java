package org.sirix.fs;

/**
 * Rudimentary shell to interact with a file hierarchy stored in XML.
 * 
 * @author BaseX Team 2011, BSD License
 * @author Alexander Holupirek <alex@holupirek.de>
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class Shell {

  // /** Shell prompt. */
  // private static final String PS1 = "[sirixfs] $ ";
  //
  // /** {@link Scanner} reference on STDIN. */
  // private static final Scanner SCANNER = new Scanner(System.in);
  //
  // /** Shell command description. */
  // @Retention(RetentionPolicy.RUNTIME)
  // @Target(ElementType.METHOD)
  // public @interface Command {
  // /** Description of expected arguments. */
  // String args() default "";
  // /** Shortcut key for command. */
  // char shortcut();
  // /** Short help message. */
  // String help();
  // }
  //
  // /**
  // * Private constructor.
  // */
  // private Shell() {
  //
  // }
  //
  // /**
  // * Factory method to create a shell instance.
  // *
  // * @return {@link Shell} reference
  // */
  // public static Shell createInstance() {
  // return new Shell();
  // }
  //
  // /**
  // * Returns the next user input.
  // *
  // * @param prompt
  // * prompt string
  // * @return user input
  // */
  // private String input(final String prompt) {
  // checkNotNull(prompt);
  // LOGWRAPPER.info(prompt);
  // return SCANNER.nextLine();
  // }
  //
  // /**
  // * Prints short help message for available commands.
  // *
  // * @param args argument vector
  // */
  // @Command(shortcut = 'h', help = "print this message")
  // public void help(final String[] args) {
  // final Method[] ms = getClass().getMethods();
  // for (final Method m : ms) {
  // if (m.isAnnotationPresent(Command.class)) {
  // final Command c = m.getAnnotation(Command.class);
  // if (args.length == 1 && args[0].charAt(0) == 'h'
  // || args.length > 1 && m.getName().equals(args[1])
  // || args.length > 1 && args[1].length() == 1
  // && c.shortcut() == args[1].charAt(0))
  // LOGWRAPPER.info("%-40s %-40s%n",
  // m.getName() + " " + c.args(),
  // c.help() + " (" + c.shortcut() + ")");
  // }
  // }
  // }
  //
  // /**
  // * Mount database as filesystem in userspace.
  // *
  // * @param args argument vector
  // */
  // @Command(shortcut = 'm'
  // , args = "<FSML database> <mountpoint>"
  // , help = "mount fsml db on path")
  // public void mount(final String[] args) {
  // if (args.length != 3) {
  // help(new String[] { "help", "mount" });
  // return;
  // }
  // final String dbname = args[1];
  // final String mountpoint = args[2];
  //
  // /* Open session. */
  // if (fs.openSession(dbname)) {
  // fs.mount(dbname, mountpoint);
  // System.out.println("Trying to mount '"
  // + dbname + "' on '" + mountpoint + "'.");
  // }
  // }
  //
  // /** Shell's main loop. */
  // private void loop() {
  // do {
  // final String[] args = input(PS1).split("\\s");
  // if (args.length != 0) {
  // exec(args);
  // }
  // } while (true);
  // }
  //
  // /**
  // * Main entry point.
  // *
  // * @param args are not processed.
  // */
  // public static void main(final String[] args) {
  // Shell.createInstance().loop();
  // }
}
