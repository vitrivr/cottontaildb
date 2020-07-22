package org.vitrivr.cottontail.cli

import org.jline.reader.Completer
import org.jline.reader.LineReaderBuilder
import org.jline.reader.impl.completer.AggregateCompleter
import org.jline.reader.impl.completer.StringsCompleter
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer
import java.io.IOException
import java.util.ArrayList
import java.util.regex.Pattern

/**
 * Command line interface instance.  Setup and general parsing. Actual commands are implemented as
 * dedicated classes.
 *
 * This is basically a port of https://github.com/lucaro/DRES 's CLI
 *
 * @author Loris Sauter
 * @version 1.0
 */
object Cli {

    /** The default prompt -- just fancification */
    private const val PROMPT = "cottontaildb>"

    /** CottotanilGrpcServer instance. */
    lateinit var cottontailServer: CottontailGrpcServer

    /**
     * Blocking REPL of the CLI
     */
    fun loop(host:String = "localhost", port:Int = 1865){
        val cmds = CottontailCommand(host, port)

        var terminal: Terminal? = null
        try{
            terminal = TerminalBuilder.terminal()
        }catch(e:IOException){
            error("Could not init terminal for reason:\n" +
                    "${e.message}\n" +
                    "Exiting...")
        }

        val completer: Completer = AggregateCompleter(
                StringsCompleter("help"),
                StringsCompleter(cmds.registeredSubcommandNames())
        )

        val lineReader = LineReaderBuilder.builder().terminal(terminal).completer(completer).build()

        while(true){
            val line = lineReader.readLine(PROMPT).trim()
            if(line.toLowerCase() == "help"){
                println(cmds.getFormattedHelp())
                continue
            }
            if(line.isBlank()){
                continue
            }
            try{
                cmds.parse(splitLine(line))
            }catch(e:Exception){
                when (e) {
                    is com.github.ajalt.clikt.core.NoSuchSubcommand -> println("command not found")
                    is com.github.ajalt.clikt.core.PrintHelpMessage -> println(e.command.getFormattedHelp())
                    is com.github.ajalt.clikt.core.MissingParameter -> println(e.localizedMessage)
                    is com.github.ajalt.clikt.core.NoSuchOption -> println(e.localizedMessage)
                    else -> e.printStackTrace()
                }
            }
        }

    }

    val lineSplitRegex: Pattern = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'")

    //based on https://stackoverflow.com/questions/366202/regex-for-splitting-a-string-using-space-when-not-surrounded-by-single-or-double/366532
    private fun splitLine(line: String?): List<String> {
        if (line == null || line.isEmpty()) {
            return emptyList()
        }
        val matchList: MutableList<String> = ArrayList()
        val regexMatcher = lineSplitRegex.matcher(line)
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                matchList.add(regexMatcher.group(1))
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                matchList.add(regexMatcher.group(2))
            } else {
                // Add unquoted word
                matchList.add(regexMatcher.group())
            }
        }
        return matchList
    }

    fun stopServer(){
        if (::cottontailServer.isInitialized){
            cottontailServer.stop()
        }
    }
}