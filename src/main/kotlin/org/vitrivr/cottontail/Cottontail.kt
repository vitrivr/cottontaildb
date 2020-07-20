package org.vitrivr.cottontail

import kotlinx.serialization.UnstableDefault
import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.Cli
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Entry point for Cottontail DB demon.
 *
 * @param args Program arguments.
 */
@UnstableDefault
fun main(args: Array<String>) {

    /* Check the argumetns */
    when (args.size) {
        1 -> {
            /* What is it? is it cli? */
            if (args[0].toLowerCase() == "cli" || args[0].toLowerCase() == "prompt") {
                startCli()
            } else {
                /* Check, if args were set properly. */
                val path =
                        if (args.isEmpty()) {
                            System.err.println("No config path specified, taking default config at config.json")
                            "config.json"
                        } else {
                            args[0]
                        }
                startDB(path)
            }
        }
        3 -> {
            if (args[0].toLowerCase() == "cli" || args[0].toLowerCase() == "prompt") {
                startCli(args[1], args[2].toInt())
            } else {
                printCliHelp()
            }
        }
        else -> {
            /* Start DB as default case */
            System.err.println("No config path specified, taking default config at config.json")
            startDB()
        }
    }
}

/**
 * Traditional cottontaildb server startup
 */
fun startDB(configPath: String = "config.json") {
    /* Load config file and start Cottontail DB. */
    Files.newBufferedReader(Paths.get(configPath)).use { reader ->
        val config = Json.parse(Config.serializer(), reader.readText())
        val catalogue = Catalogue(config)
        val engine = ExecutionEngine(config.executionConfig)
        val server = CottontailGrpcServer(config.serverConfig, catalogue, engine)
        server.start()
        if (config.cli) {
            /* Start local cli */
            Cli.cottontailServer = server
            startCli("localhost", config.serverConfig.port)
        } else {
            while (server.isRunning) {
                Thread.sleep(1000)
            }
        }
    }
}

/**
 * Starts the (blocking) CLI REPL
 */
fun startCli(host: String = "localhost", port: Int = 1865) {
    println("Starting CLI...")
    Cli.loop(host, port)
}

/**
 * Prints info about how to start w/ cli
 */
fun printCliHelp() {
    println("To start the CLI start cottontaildb use\n" +
            "$> cli [<host>] [<port>]")
}