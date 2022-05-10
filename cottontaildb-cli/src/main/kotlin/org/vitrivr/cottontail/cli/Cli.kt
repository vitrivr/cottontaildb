package org.vitrivr.cottontail.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.output.CliktHelpFormatter
import com.github.ajalt.clikt.output.HelpFormatter
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.jline.builtins.Completers
import org.jline.builtins.Completers.TreeCompleter.node
import org.jline.reader.Completer
import org.jline.reader.EndOfFileException
import org.jline.reader.LineReaderBuilder
import org.jline.reader.UserInterruptException
import org.jline.reader.impl.completer.ArgumentCompleter
import org.jline.terminal.TerminalBuilder
import org.vitrivr.cottontail.cli.entity.*
import org.vitrivr.cottontail.cli.query.*
import org.vitrivr.cottontail.cli.schema.*
import org.vitrivr.cottontail.cli.system.KillTransactionCommand
import org.vitrivr.cottontail.cli.system.ListLocksCommand
import org.vitrivr.cottontail.cli.system.ListTransactionsCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.ListEntities
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import java.io.IOException
import java.util.regex.Pattern
import kotlin.time.ExperimentalTime

/**
 * Command line interface instance.  Setup and general parsing. Actual commands are implemented as
 * dedicated classes.
 *
 * This is basically a port of https://github.com/lucaro/DRES 's CLI
 *
 * @author Loris Sauter
 * @version 1.1.0
 */
@ExperimentalTime
class Cli(private val host: String = "localhost", private val port: Int = 1865) {

    companion object {
        /** The default prompt -- just fancification */
        private const val PROMPT = "\uD83E\uDD55"

        /** RegEx for splitting input lines. */
        private val LINE_SPLIT_REGEX: Pattern = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'")
    }

    /** The [ManagedChannel] used to connect to Cottontail DB. */
    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(this@Cli.host, this@Cli.port)
        .enableFullStreamDecompression()
        .usePlaintext()
        .build()

    /** The [SimpleClient] used to access Cottontail DB. */
    private val client = SimpleClient(this.channel)

    /** Generates a new instance of [CottontailCommand]. */
    private val clikt = CottontailCommand()

    /** Register [ArgumentCompleter] for tope level CLI. */
    private val completer: Completer = initCompleter()

    /** Flag indicating whether [Cli] has been stopped. */
    @Volatile
    private var stopped: Boolean = false

    /**
     * Tries to execute the given CLI command.
     */
    fun execute(command: String) = try {
        this.clikt.parse(splitLine(command))
        println()
    } catch (e: Exception) {
        when (e) {
            is com.github.ajalt.clikt.core.PrintHelpMessage -> println(e.command.getFormattedHelp())
            is com.github.ajalt.clikt.core.NoSuchSubcommand,
            is com.github.ajalt.clikt.core.MissingArgument,
            is com.github.ajalt.clikt.core.MissingOption,
            is com.github.ajalt.clikt.core.BadParameterValue,
            is com.github.ajalt.clikt.core.NoSuchOption,
            is com.github.ajalt.clikt.core.UsageError -> println(e.localizedMessage)
            is StatusException, /* Exceptions reported by Cottontail DB via gRPC. */
            is StatusRuntimeException -> println(e.localizedMessage)
            else -> println(e.printStackTrace())
        }
    }

    /**
     * Blocking REPL of the CLI
     */
    fun loop() {
        val terminal = try {
            TerminalBuilder.builder().jna(true).build()
        } catch (e: IOException) {
            System.err.println("Could not initialize terminal: ${e.message}. Ending C(arrot)LI...")
            return
        }

        /* Start CLI loop. */
        val lineReader = LineReaderBuilder.builder().terminal(terminal).completer(this.completer).appName("C(arrot)LI").build()
        println("Connected C(arrot)LI to Cottontail DB at ${this.host}:${this.port}. Happy searching...")

        while (!this.stopped) {
            /* Catch ^D end of file as exit method */
            val line = try {
                lineReader.readLine(PROMPT).trim()
            } catch (e: EndOfFileException) {
                System.err.println("Could not read from terminal.")
                break
            } catch (e: UserInterruptException) {
                System.err.println(" C(arrot)LI was interrupted by the user (Ctrl-C).")
                break
            }

            if (line.lowercase() == "help") {
                println(clikt.getFormattedHelp())
                continue
            }
            if (line.isBlank()) {
                continue
            }

            /* Execute command. */
            this.execute(line)

            /* Sleep for a few milliseconds. */
            Thread.sleep(100)
        }
    }

    /**
     * Initializes the necessary [Completer]s.
     */
    private fun initCompleter(): Completer {
        val schemata = mutableListOf<String>()
        val entities = mutableListOf<String>()
        try {
            for (schema in this.client.list(ListSchemas())) {
                val sfqn = schema.asString(0)!!

                /* Schema name with and w/o 'warren.'. */
                schemata.add(sfqn)
                schemata.add("warren.$sfqn")

                for (entity in this.client.list(ListEntities(sfqn))) {
                    val efqn = node(entity.asString(0)!!)

                    /* Entity name with and w/o 'warren.'. */
                    entities.add("$sfqn.$efqn")
                    entities.add("warren.$sfqn.$efqn")
                }
            }
        } catch (e: StatusRuntimeException) {
            System.err.println("Failed to fetch schema from Cottontail DB; some commands may be unavailable!")
        }

        val nodes = this.clikt.registeredSubcommands().map { ocmd -> /* Outer command. */
            node(ocmd.commandName, *ocmd.registeredSubcommands().map { icmd -> /* Inner command. */
                when {
                    icmd is AbstractCottontailCommand.Schema && icmd.expand -> {
                        node(icmd.commandName, *schemata.map { node(it) }.toTypedArray())
                    }
                    icmd is AbstractCottontailCommand.Query && icmd.expand ||
                    icmd is AbstractCottontailCommand.Entity && icmd.expand -> {
                        node(icmd.commandName, *entities.map { node(it) }.toTypedArray())
                    }
                    else -> {
                        node(icmd.commandName)
                    }
                }
            }.toTypedArray())
        }

        return Completers.TreeCompleter(nodes)
    }


    //based on https://stackoverflow.com/questions/366202/regex-for-splitting-a-string-using-space-when-not-surrounded-by-single-or-double/366532
    private fun splitLine(line: String?): List<String> {
        if (line == null || line.isEmpty()) {
            return emptyList()
        }
        val matchList: MutableList<String> = ArrayList()
        val regexMatcher = LINE_SPLIT_REGEX.matcher(line)
        while (regexMatcher.find()) {
            when {
                regexMatcher.group(1) != null -> matchList.add(regexMatcher.group(1))
                regexMatcher.group(2) != null -> matchList.add(regexMatcher.group(2))
                else -> matchList.add(regexMatcher.group())
            }
        }
        return matchList
    }

    /**
     * Stops the CLI loop.
     */
    fun stop() {
        this.stopped = true
    }

    /**
     * Wrapper class and single access point to the actual commands. Also, provides the gRPC bindings.
     *
     * <strong>How to add more commands:</strong>
     * <ul>
     *     <li>Write dedicated inner class (extend [AbstractEntityCommand] if it's an entity specific command, otherwise [AbstractCottontailCommand])</li>
     *     <li>Add this command to subcommands call in line 32ff (init of this class)</li>
     * </ul>
     *
     * @author Loris Sauter
     * @version 2.0.0
     */
    @ExperimentalTime
    inner class CottontailCommand : NoOpCliktCommand(name = "cottontail", help = "The base command for all CLI commands.") {




        /** A list of aliases: mapping of alias name to commands */
        override fun aliases(): Map<String, List<String>> {
            /* List of top-level aliases */
            return mapOf(
                "sls" to listOf("schema", "all"),
                "els" to listOf("schema", "list"),
                "tls" to listOf("system", "transactions"),
                "ls" to listOf("entity", "all"),
                "edelete" to listOf("entity", "drop"),
                "edel" to listOf("entity", "drop"),
                "erm" to listOf("entity", "drop"),
                "eremove" to listOf("entity", "drop"),
                "delete" to listOf("schema", "drop"),
                "del" to listOf("schema", "drop"),
                "rm" to listOf("schema", "drop"),
                "remove" to listOf("schema", "drop"),
                "li" to listOf("entity", "list-indices"),
                "quit" to listOf("stop"),
                "exit" to listOf("stop"),
                "schemas" to listOf("schema"),
                "entities" to listOf("entity"),
                "trb" to listOf("system", "rollback"),
            )
        }

        init {
            context { helpFormatter = CliHelpFormatter() }
            subcommands(
                /* Entity related commands. */
                object : NoOpCliktCommand(
                    name = "entity",
                    help = "Groups commands that act on Cottontail DB entities. Usually requires the entity's qualified name.",
                    epilog = "Entity related commands usually have the form: entity <command> <name>, `entity about schema_name.entity_name. Check help for command specific parameters.",
                    invokeWithoutSubcommand = true,
                    printHelpOnEmptyArgs = true
                ) {
                    override fun aliases(): Map<String, List<String>> {
                        /* List of entity aliases: entity <alias> */
                        return mapOf(
                            "ls" to listOf("list"),
                            "list-indexes" to listOf("list-indices")
                        )
                    }
                }.subcommands(
                    AboutEntityCommand(this@Cli.client),
                    ClearEntityCommand(this@Cli.client),
                    CreateEntityCommand(this@Cli.client),
                    DropEntityCommand(this@Cli.client),
                    TruncateEntityCommand(this@Cli.client),
                    ListAllEntitiesCommand(this@Cli.client),
                    OptimizeEntityCommand(this@Cli.client),
                    CreateIndexCommand(this@Cli.client),
                    DropIndexCommand(this@Cli.client),
                    DumpEntityCommand(this@Cli.client),
                    ImportDataCommand(this@Cli.client)
                ),

                /* Schema related commands. */
                object : NoOpCliktCommand(
                    name = "schema",
                    help = "Groups commands that act on Cottontail DB  schemas. Usually requires the schema's qualified name",
                    epilog = "Schema related commands usually have the form: schema <command> <name>, e.g., `schema list schema_name` Check help for command specific parameters.",
                    invokeWithoutSubcommand = true,
                    printHelpOnEmptyArgs = true
                ) {
                    override fun aliases(): Map<String, List<String>> {
                        return mapOf(
                            "ls" to listOf("list")
                        )
                    }
                }.subcommands(
                    CreateSchemaCommand(this@Cli.client),
                    DropSchemaCommand(this@Cli.client),
                    DumpSchemaCommand(this@Cli.client),
                    ListAllSchemaCommand(this@Cli.client),
                    ListEntitiesCommand(this@Cli.client)
                ),

                /* Transaction related commands. */
                object : NoOpCliktCommand(
                    name = "query",
                    help = "Groups commands that can be used to query Cottontail DB.",
                    epilog = "Transaction related commands usually have the form: query <command>, e.g., `query execute`.",
                    invokeWithoutSubcommand = true,
                    printHelpOnEmptyArgs = true
                ) {}.subcommands(
                    CountEntityCommand(this@Cli.client),
                    PreviewEntityCommand(this@Cli.client),
                    FindInEntityCommand(this@Cli.client),
                    ExecuteQueryCommand(this@Cli.client),
                    DistinctColumnQueryCommand(this@Cli.client)
                ),

                /* Transaction related commands. */
                object : NoOpCliktCommand(
                    name = "system",
                    help = "Groups commands that act on the Cottontail DB on a system level.",
                    epilog = "Transaction related commands usually have the form: system <command> <txId>, e.g., `transaction list-transactions`.",
                    invokeWithoutSubcommand = true,
                    printHelpOnEmptyArgs = true
                ) {
                    override fun aliases(): Map<String, List<String>> {
                        return mapOf(
                            "ls" to listOf("list"),
                            "tls" to listOf("transactions"),
                            "list-transactions" to listOf("transactions"),
                            "rb" to listOf("rollback")
                        )
                    }
                }.subcommands(
                    ListTransactionsCommand(this@Cli.client),
                    ListLocksCommand(this@Cli.client),
                    KillTransactionCommand(this@Cli.client)
                ),

                /* General commands. */
                StopCommand()
            )
        }

        /**
         * Dedicated help formatter
         */
        inner class CliHelpFormatter : CliktHelpFormatter() {
            override fun formatHelp(
                prolog: String,
                epilog: String,
                parameters: List<HelpFormatter.ParameterHelp>,
                programName: String
            ): String = buildString {
                if (programName.contains(" ")) {
                    addUsage(parameters, programName.split(" ")[1]) // hack to not include the base command
                } else {
                    addUsage(parameters, programName)
                }
                addOptions(parameters)
                addArguments(parameters)
                addCommands(parameters)
                if (programName.endsWith("cottontail")) { // hack for beautification
                    addEpilog(
                        "              ((`\\\u0085            ___ \\\\ '--._\u0085         .'`   `'    o  )\u0085        /    \\   '. __.'\u0085       _|    /_  \\ \\_\\_\u0085      {_\\______\\-'\\__\\_\\\u0085\u0085" +
                                "by jks from https://www.asciiart.eu/animals/rabbits"
                    )
                }
            }
        }

        /**
         * Stops the entire application
         */
        inner class StopCommand : CliktCommand(name = "stop", help = "Stops this CLI.") {
            override fun run() {
                println("Stopping C(arrot)LI now...")
                this@Cli.stop()
            }
        }
    }
}
