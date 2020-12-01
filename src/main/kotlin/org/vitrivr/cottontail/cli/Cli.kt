package org.vitrivr.cottontail.cli

import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.output.CliktHelpFormatter
import com.github.ajalt.clikt.output.HelpFormatter
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.jline.reader.EndOfFileException
import org.jline.reader.LineReaderBuilder
import org.jline.reader.impl.completer.AggregateCompleter
import org.jline.reader.impl.completer.ArgumentCompleter
import org.jline.reader.impl.completer.NullCompleter
import org.jline.reader.impl.completer.StringsCompleter
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import org.vitrivr.cottontail.cli.entity.*
import org.vitrivr.cottontail.cli.schema.ListAllEntitiesCommand
import org.vitrivr.cottontail.cli.schema.ListEntitiesCommand
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.io.IOException
import java.util.*
import java.util.regex.Pattern
import kotlin.time.ExperimentalTime

/**
 * Command line interface instance.  Setup and general parsing. Actual commands are implemented as
 * dedicated classes.
 *
 * This is basically a port of https://github.com/lucaro/DRES 's CLI
 *
 * @author Loris Sauter
 * @version 1.0
 */
@ExperimentalTime
class Cli(val host: String = "localhost", val port: Int = 1865) {

    companion object {
        /** The default prompt -- just fancification */
        private const val PROMPT = "cottontaildb> "

        /** RegEx for splitting input lines. */
        private val LINE_SPLIT_REGEX: Pattern = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'")
    }

    /** Generates a new instance of [CottontailCommand]. */
    private val clikt = CottontailCommand()

    /** [DelegateCompleter] for auto completion. */
    private val completer: DelegateCompleter = DelegateCompleter(AggregateCompleter(StringsCompleter("help")))

    /** Flag indicating whether [Cli] has been stopped. */
    @Volatile
    private var stopped: Boolean = false

    /**
     * Add the given list of completion candidates to completion
     */
    fun updateCompletion(strings: List<String>) {
        this.completer.delegate = AggregateCompleter(
                StringsCompleter("help"),
                StringsCompleter(clikt.registeredSubcommandNames()),
                StringsCompleter(strings))
    }

    /**
     * Adds dedicated argument completers to the completion
     */
    fun updateArgumentCompletion(schemata: List<String>, entities: List<String>) {
        val args = ArgumentCompleter(
                StringsCompleter(clikt.registeredSubcommandNames()),// should be safe to call
                StringsCompleter(schemata),
                StringsCompleter(entities),
                NullCompleter()
        )
        args.setStrictCommand(true)
        args.isStrict = false
        completer.delegate = AggregateCompleter(
                StringsCompleter("help"),
                args
        )
    }


    /**
     * Resets the autocompletion.
     * Resetting means, that in case the commands are already loaded, these are set plus "help"
     * Otherwise only "help" is added to the completion.
     */
    fun resetCompletion() {
        this.completer.delegate = AggregateCompleter(StringsCompleter("help"), StringsCompleter(clikt.registeredSubcommandNames()))
    }

    /**
     * Blocking REPL of the CLI
     */
    fun loop() {
        val terminal: Terminal?
        try {
            terminal = TerminalBuilder.terminal()
        } catch (e: IOException) {
            error("Could not init terminal for reason:\n" +
                    "${e.message}\n" +
                    "Exiting...")
        }

        /* Initialize auto complete. */
        this.clikt.initCompletion()

        /* Start CLI loop. */
        val lineReader = LineReaderBuilder.builder().terminal(terminal).completer(completer).build()
        while (!this.stopped) {
            /* Catch ^D end of file as exit method */
            val line = try {
                lineReader.readLine(PROMPT).trim()
            } catch (e: EndOfFileException) {
                "stop"
            }
            if (line.toLowerCase() == "help") {
                println(clikt.getFormattedHelp())
                continue
            }
            if (line.isBlank()) {
                continue
            }
            try {
                this.clikt.parse(splitLine(line))
                println()
            } catch (e: Exception) {
                when (e) {
                    is com.github.ajalt.clikt.core.PrintHelpMessage -> println(e.command.getFormattedHelp())
                    is com.github.ajalt.clikt.core.NoSuchSubcommand,
                    is com.github.ajalt.clikt.core.MissingArgument,
                    is com.github.ajalt.clikt.core.MissingOption,
                    is com.github.ajalt.clikt.core.BadParameterValue,
                    is com.github.ajalt.clikt.core.NoSuchOption -> println(e.localizedMessage)
                    is StatusException, /* Exceptions reported by Cottontail DB via gRPC. */
                    is StatusRuntimeException -> println(e.localizedMessage)
                    else -> println(e.printStackTrace())
                }
            }

            /* Sleep for a few milliseconds. */
            Thread.sleep(100)
        }

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
     * @version 1.0.2
     */
    @ExperimentalTime
    inner class CottontailCommand : NoOpCliktCommand(name = "cottontail", help = "The base command") {

        /** The [ManagedChannel] used to conect to Cottontail DB. */
        private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(this@Cli.host, this@Cli.port).usePlaintext().build()

        /** The [CottonDQLGrpc.CottonDQLBlockingStub] used for querying Cottontail DB. */
        private val dqlService = CottonDQLGrpc.newBlockingStub(this.channel)

        /** The [CottonDDLGrpc.CottonDDLBlockingStub] used for changing Cottontail DB DBOs. */
        private val ddlService = CottonDDLGrpc.newBlockingStub(this.channel)

        init {
            context { helpFormatter = CliHelpFormatter() }
            subcommands(
                    /* DQL related commands. */
                    PreviewEntityCommand(this.dqlService),
                    CountEntityCommand(this.dqlService),
                    FindInEntityCommand(this.dqlService),

                    /* DDL related commands. */
                    ListAllEntitiesCommand(this.ddlService),
                    ListEntitiesCommand(this.ddlService),
                    AboutEntityCommand(this.ddlService),
                    DropEntityCommand(this.ddlService),
                    OptimizeEntityCommand(this.ddlService),

                    /* CLI related commands. */
                    StopCommand()
            )
        }

        /**
         * Initializes the auto completion for entity names.
         */
        fun initCompletion() {
            val schemata = mutableListOf<CottontailGrpc.Schema>()
            val entities = mutableListOf<CottontailGrpc.Entity>()
            this@CottontailCommand.ddlService.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
                schemata.add(_schema)
                this@CottontailCommand.ddlService.listEntities(_schema).forEach { _entity ->
                    entities.add(_entity)
                }
            }
            this@Cli.updateArgumentCompletion(schemata = schemata.map { it.name }, entities = entities.map { it.name })
        }

        /**
         * Dedicated help formatter
         */
        inner class CliHelpFormatter : CliktHelpFormatter() {
            override fun formatHelp(
                    prolog: String,
                    epilog: String,
                    parameters: List<HelpFormatter.ParameterHelp>,
                    programName: String): String = buildString {
                if (programName.contains(" ")) {
                    addUsage(parameters, programName.split(" ")[1]) // hack to not include the base command
                } else {
                    addUsage(parameters, programName)
                }
                addOptions(parameters)
                addArguments(parameters)
                addCommands(parameters)
            }
        }

        /**
         * Stops the entire application
         */
        inner class StopCommand : AbstractCottontailCommand(name = "stop", help = "Stops the database server and this CLI") {
            override fun exec() {
                println("Stopping Cottontail DB now...")
                this@Cli.stop()
            }
        }
    }
}