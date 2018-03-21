package org.sirix.fs.commands;

/**
 * Interface which all commands have to implement (Command pattern).
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface Command {
  /** Execute command. */
  void execute();
}
