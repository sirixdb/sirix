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

package org.sirix.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.sirix.access.DatabaseImpl;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;

/**
 * This class acts as a commando line interface to navigate through a sirix
 * structure.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class CommandoLineExplorer {

	private CommandoLineExplorer() {
		// Not used over here.
	}

	/**
	 * DELIM-Constant for commands.
	 */
	private static final String COMMANDDELIM = ":";

	/**
	 * Finding file for a given command.z.
	 * 
	 * @param mCommandLine
	 *          the line to be analysed
	 * @return the corresponding file
	 */
	private static File findFile(final String mCommandLine) {
		final String[] command = mCommandLine.split(COMMANDDELIM);
		if (command.length != 2) {
			return null;
		}
		return new File(command[1]);
	}

	/**
	 * Main for all the knowledge and input.
	 * 
	 * @param args
	 *          only one arg allowed, the tnk file
	 * @throws Exception
	 *           of any kind
	 */
	public static void main(final String[] args) throws Exception {
		Database database = null;
		Session session = null;
		NodeReadTrx rtx = null;
		if (args.length > 0) {
			int revision = 0;
			if (args.length > 1) {
				revision = Integer.parseInt(args[1]);
			}

			final File file = new File(args[0]);
			final DatabaseConfiguration config = new DatabaseConfiguration(file);
			Databases.createDatabase(config);
			database = Databases.openDatabase(file);
			database.createResource(new ResourceConfiguration.Builder("TMP", config)
					.build());
			session = database.getSession(new SessionConfiguration.Builder("TMP")
					.build());
			if (revision != 0) {
				rtx = session.beginNodeWriteTrx();
			} else {
				assert session != null;
				rtx = session.beginNodeReadTrx(revision);
			}
		} else {
			System.out
					.println("Usage: java sirixCommandoLineExplorer \"tnk-file\" [revision] "
							+ "(if revision not given, explorer works in write mode");
			System.exit(-1);
		}

		try {
			System.out.println("Welcome to TTCommand");
			System.out.println("Type in \"help\" for help for usage..");

			String line = null;
			final BufferedReader bufferIn = new BufferedReader(new InputStreamReader(
					System.in));
			System.out.print(">");
			while ((line = bufferIn.readLine()) != null) {

				final Command command = Command.toCommand(line);
				switch (command) {
				case LOGIN:
					if (rtx != null) {
						rtx.close();
					}
					if (session != null) {
						session.close();
					}
					final File file = findFile(line);
					if (file != null) {
						database = Databases.openDatabase(file);
						session = database.getSession(new SessionConfiguration.Builder(
								"TMP").build());
						rtx = session.beginNodeReadTrx();
						System.out.println(command.executeCommand(rtx));
					} else {
						System.out.println("Invalid path to tt-file! Please use other!");
					}
					break;
				case LOGOUT:
					System.out.println(command.executeCommand(rtx));
					if (rtx != null) {
						rtx.close();
					}
					if (session != null) {
						session.close();
					}
					break;
				case EXIT:
					System.out.println(command.executeCommand(rtx));
					if (rtx != null) {
						rtx.close();
					}
					if (session != null) {
						session.close();
					}
					System.exit(1);
					break;
				default:
					if (session == null || rtx == null) {
						System.out.println(new StringBuilder(
								"No database loaded!, Please use ")
								.append(Command.LOGIN.mCommand).append(" to load tt-database")
								.toString());
					} else {
						System.out.println(command.executeCommand(rtx));
					}
					System.out.print(">");
				}
			}

		} catch (final SirixException e) {
			if (rtx != null) {
				rtx.close();
			}
			if (session != null) {
				session.close();
			}
			System.exit(-1);
		}

	}

	/**
	 * Enums for known Commands.
	 * 
	 * @author Sebastian Graf, University of Konstanz
	 * 
	 */
	private enum Command {
		HELP("help") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				final StringBuilder builder = new StringBuilder("Help for ");
				if (mParameter.equals(INFO.mCommand)) {
					builder.append("info:\n");
					builder.append("prints out nodeKey, child count, parent key, ")
							.append("first child key, left sibling key, right sibling key\n");
				} else if (mParameter.equals(CONTENT.mCommand)) {
					builder.append("content:\n");
					builder.append("prints out kind of node plus relevant content\n");
				} else if (mParameter.equals(LOGOUT.mCommand)) {
					builder.append("logout:\n");
					builder.append("Logout from database\n");
				} else if (mParameter.equals(LOGIN.mCommand)) {
					builder.append("login:\n");
					builder.append("Parameter is the path to the tt-database").append(
							"\"login:[path]\"\n");
				} else if (mParameter.equals(EXIT.mCommand)) {
					builder.append("exit:\n");
					builder.append("Exits the program\n");
				} else if (mParameter.equals(MOVE.mCommand)) {
					builder.append("move:\n");
					builder.append("Below a concrete parameter list\n");
					builder.append("up\t\t:\tGo to father if possible\n");
					builder.append("down\t\t:\tGo to first child if possible\n");
					builder.append("left\t\t:\tGo to left sibling if possible\n");
					builder.append("right\t\t:\tGo to right sibling if possible\n");
					builder.append("root\t\t:\tGo to document root if possible\n");
					builder
							.append("[nodekey]\t:\tGo to specified node key if possible, ")
							.append("[nodekey] has to be a long\n");
				} else {
					builder.append("common usage\n Usage: [COMMAND]:[PARAMETER]\n");
					builder.append("For concrete parameter-list, type ")
							.append(HELP.mCommand).append(COMMANDDELIM).append("[COMMAND]\n");
					builder.append("Below a list of all commands:\n");
					builder.append(LOGIN.mCommand).append("\t:\t")
							.append("Login into database.\n");
					builder.append(LOGOUT.mCommand).append("\t:\t")
							.append("Logout from database.\n");
					builder.append(EXIT.mCommand).append("\t:\t")
							.append("Exits the programm.\n");
					builder.append(INFO.mCommand).append("\t:\t")
							.append("Offers info about the current node.\n");
					builder.append(MOVE.mCommand).append("\t:\t")
							.append("Moving to given node.\n");
				}
				return builder.toString();
			}
		},
		CONTENT("content") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				final StringBuilder builder = new StringBuilder("Kind: ");
				switch (mCurrentRtx.getKind()) {
				case ELEMENT:
					builder.append("Element\n");
					builder.append(mCurrentRtx.getName());
					break;
				case ATTRIBUTE:
					builder.append("Attribute\n");
					builder.append(mCurrentRtx.getName());
					builder.append("=");
					builder.append(mCurrentRtx.getValue());
					break;
				case TEXT:
					builder.append("Text\n");
					builder.append(mCurrentRtx.getValue());
					break;
				case NAMESPACE:
					builder.append("Namespace\n");
					builder.append(mCurrentRtx.getName());
					break;
				case PROCESSING:
					builder.append("Processing instruction\n");
					break;
				case COMMENT:
					builder.append("Comment\n");
					break;
				case DOCUMENT_ROOT:
					builder.append("Document Root\n");
					break;
				default:
					builder.append("unknown!");
				}
				return builder.toString();
			}
		},
		INFO("info") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				final StringBuilder builder = new StringBuilder();
				builder.append(mCurrentRtx.toString());
				return builder.toString();
			}
		},
		LOGIN("login") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				return new StringBuilder("Loggin into database ").append(mParameter)
						.append("\n").toString();
			}
		},
		LOGOUT("logout") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				return new StringBuilder("Logout from database.").toString();
			}
		},
		EXIT("exit") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				return new StringBuilder("Exiting the program.").toString();
			}
		},
		MOVE("move") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				boolean succeed = false;
				final StringBuilder builder = new StringBuilder("Move to ");
				if (mParameter.equals("up")) {
					builder.append("parent ");
					succeed = mCurrentRtx.moveToParent().hasMoved();
				} else if (mParameter.equals("down")) {
					builder.append("first child ");
					succeed = mCurrentRtx.moveToFirstChild().hasMoved();
				} else if (mParameter.equals("right")) {
					builder.append("right sibling ");
					succeed = mCurrentRtx.moveToRightSibling().hasMoved();
				} else if (mParameter.equals("left")) {
					builder.append("left sibling ");
					succeed = mCurrentRtx.moveToLeftSibling().hasMoved();
				} else if (mParameter.equals("root")) {
					builder.append("document root ");
					succeed = mCurrentRtx.moveToDocumentRoot().hasMoved();
				} else {
					try {
						final long nodeKey = Long.parseLong(mParameter);
						builder.append("node with key ").append(nodeKey).append(" ");
						succeed = mCurrentRtx.moveTo(nodeKey).hasMoved();
					} catch (final NumberFormatException e) {
						builder.append("invalid node ");
						succeed = false;
					}
				}
				if (succeed) {
					builder.append("succeeded");
				} else {
					builder.append("not succeeded");
				}

				return builder.toString();
			}
		},
		MODIFICATION("modification") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				final StringBuilder builder = new StringBuilder("Insert ");
				try {
					if (mCurrentRtx instanceof NodeWriteTrx) {
						final NodeWriteTrx wtx = (NodeWriteTrx) mCurrentRtx;

						if (mParameter.equals("commit")) {
							wtx.commit();
							builder.append(
									" operation: commit succeed. New revision-number is ")
									.append(wtx.getRevisionNumber());
						} else if (mParameter.equals("abort")) {
							wtx.abort();
							builder.append(
									" operation: abort succeed. Old revision-number is ").append(
									wtx.getRevisionNumber());
						}

					} else {
						builder.append(" not succeed, Please login with write-right "
								+ "(that means without revision parameter");
					}
				} catch (final SirixException exc) {
					builder.append(" throws exception: ").append(exc);
				}
				return builder.toString();
			}
		},
		NOVALUE("") {
			@Override
			String executeCommand(final NodeReadTrx mCurrentRtx,
					final String mParameter) {
				return new StringBuilder("Command not known. Try ")
						.append(Command.HELP.getCommand()).append(" for known commands!")
						.toString();
			}
		};

		private final String mCommand;

		private String mParameter = "";

		Command(final String paramCommand) {
			mCommand = paramCommand;
		}

		private static Command toCommand(final String mCommandString) {
			try {
				final String[] commandStrings = mCommandString.split(COMMANDDELIM);
				final Command command = valueOf(commandStrings[0].toUpperCase());
				if (commandStrings.length == 2) {
					command.setAdvise(commandStrings[1].toLowerCase());
				}

				return command;
			} catch (final Exception e) {
				return NOVALUE;
			}
		}

		private String executeCommand(final NodeReadTrx read) {
			return executeCommand(read, mParameter);
		}

		/**
		 * Executing a command.
		 * 
		 * @param mCurrentRtx
		 *          on which the command should be executed
		 * @param parameter
		 *          Parameter to executed
		 * @return a String as a result
		 */
		abstract String executeCommand(final NodeReadTrx mCurrentRtx,
				final String parameter);

		/**
		 * Getter for field command.
		 * 
		 * @return the command
		 */
		private String getCommand() {
			return mCommand;
		}

		/**
		 * Setter for field advise.
		 * 
		 * @param paramParameter
		 *          to be set.
		 */
		private void setAdvise(final String paramParameter) {
			mParameter = paramParameter;
		}

	}
}
