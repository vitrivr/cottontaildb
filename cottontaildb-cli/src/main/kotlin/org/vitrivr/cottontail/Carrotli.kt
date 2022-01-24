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
    /* Extract host and port (if provided). */
    val host = if (args.isNotEmpty()) { args[0] } else { "127.0.0.1" }
    val port = if (args.size > 1) { args[1].toInt() } else { 1865 }

    /* Start CLI. */
    Cli(host, port).loop()
}