package org.vitrivr.cottontail

import org.vitrivr.cottontail.cli.Cli
import kotlin.time.ExperimentalTime

/**
 * Entry point for C(arrot)CLI.
 *
 * @param args Program arguments.
 */
@OptIn(ExperimentalTime::class)
fun main(args: Array<String>) {
    /* Parse custom host name. */
    val hostArgument = args.find { s -> s.startsWith("--host=") }
    val host = if (hostArgument != null) {
        hostArgument.split('=')[1]
    } else {
        "127.0.0.1"
    }

    /* Parse custom port argument. */
    val portArgument = args.find { s -> s.startsWith("--port=") }
    val port = if (portArgument != null) {
        portArgument.split('=')[1].toInt()
    } else {
        1865
    }

    /* Parse custom command. */
    val commandArgument = args.find { s -> s.startsWith("--command=") }
    if (commandArgument != null) {
        val command = commandArgument.subSequence(commandArgument.indexOf('=') + 1, commandArgument.length).toString()
        Cli(host, port).execute(command)
    } else {
        Cli(host, port).loop()
    }
}