package org.vitrivr.cottontail

import kotlinx.serialization.UnstableDefault
import kotlinx.serialization.json.Json
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
    /* Check, if args were set properly. */
    val path =
            if (args.isEmpty()) {
                System.err.println("No config path specified, taking default config at config.json")
                "config.json"
            } else {
                args[0]
            }

    /* Load config file and start Cottontail DB. */
    Files.newBufferedReader(Paths.get(path)).use { reader ->
        val config = Json.parse(Config.serializer(), reader.readText())
        val catalogue = Catalogue(config)
        val engine = ExecutionEngine(config.executionConfig)
        val server = CottontailGrpcServer(config.serverConfig, catalogue, engine)
        server.start()
        while (server.isRunning) {
            Thread.sleep(1000)
        }
    }
}