package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine
import ch.unibas.dmi.dbis.cottontail.server.grpc.CottontailGrpcServer
import kotlinx.serialization.UnstableDefault

import kotlinx.serialization.json.Json.Companion.parse

import java.nio.file.Files
import java.nio.file.Paths

/**
 * Entry point for Cottontail DB demon.
 *
 * @param args Program arguments.
 */
@UnstableDefault
fun main(args: Array<String>) {
    /* Check, if args were set properly. */
    if (args.isEmpty()) {
        System.err.println("You must specify the path to the Cottontail DB configuration file (config.json) as a program argument. Shutting down...")
        System.exit(1)
    }

    /* Load config file and start Cottontail DB. */
    val path = args[0]
    Files.newBufferedReader(Paths.get(path)).use { reader ->
        val config = parse(Config.serializer(), reader.readText())
        val catalogue = Catalogue(config)
        val engine = ExecutionEngine(config.executionConfig)
        val server = CottontailGrpcServer(config.serverConfig, catalogue, engine)
        server.start()
        while(server.isRunning) {
            Thread.sleep(1000)
        }
    }
}