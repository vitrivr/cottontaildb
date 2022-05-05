package org.vitrivr.cottontail

import org.vitrivr.cottontail.cli.Cli
import kotlin.time.ExperimentalTime

/**
 * Entry point for C(arrot)LI.
 *
 * @param args Program arguments.
 */
@OptIn(ExperimentalTime::class)
fun main(args: Array<String>) {
    /* Parse custom host and port argument. */
    val host = args.find { s -> s.startsWith("--host=") }?.split('=')?.get(1) ?: "127.0.0.1"
    val port = args.find { s -> s.startsWith("--port=") }?.split('=')?.get(1)?.toIntOrNull() ?: 1865

    /* Parse custom command. */
    val commandArgument = args.find { s -> s.startsWith("--command=") }
    if (commandArgument != null) {
        val command = commandArgument.subSequence(commandArgument.indexOf('=') + 1, commandArgument.length).toString()
        Cli(host, port).execute(command)
    } else {
        Cli(host, port).loop()
    }
}