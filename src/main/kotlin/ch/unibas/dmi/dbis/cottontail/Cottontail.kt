package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine
import ch.unibas.dmi.dbis.cottontail.server.grpc.CottontailGrpcServer

import kotlinx.serialization.json.JSON

import java.nio.file.Files
import java.nio.file.Paths

object Cottontail {

    /**
     * Entry point for Cottontail DB demon.
     *
     * @param args Program arguments.
     */
    @JvmStatic
    fun main(args: Array<String>) {
        val path = args[0]
        Files.newBufferedReader(Paths.get(path)).use { reader ->
            val config = JSON.parse(Config.serializer(), reader.readText())
            val catalogue = Catalogue(config)
            val engine = ExecutionEngine(config.executionConfig)
            val server = CottontailGrpcServer(config.serverConfig, catalogue, engine)
            server.start()
            while(server.isRunning) {
                Thread.sleep(1000)
            }
        }
    }
}
