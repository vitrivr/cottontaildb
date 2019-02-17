package ch.unibas.dmi.dbis.cottontail.server.grpc

import ch.unibas.dmi.dbis.cottontail.config.ServerConfig
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine
import ch.unibas.dmi.dbis.cottontail.server.grpc.services.CottonDDLService
import ch.unibas.dmi.dbis.cottontail.server.grpc.services.CottonDMLService
import ch.unibas.dmi.dbis.cottontail.server.grpc.services.CottonDQLService

import io.grpc.ServerBuilder
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import java.util.concurrent.*

/**
 * Main server class for the GRPC endpoint provided by Cottotail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailGrpcServer(config: ServerConfig, val catalogue: Catalogue, val engine: ExecutionEngine) {

    /** The [ThreadPoolExecutor] used for handling the individual GRPC calls. */
    private val executor: ExecutorService = ThreadPoolExecutor(config.coreThreads, config.maxThreads, config.keepAliveTime, TimeUnit.MILLISECONDS, SynchronousQueue())

    /** Reference to the GRPC server. */
    private val server = ServerBuilder.forPort(config.port)
            .executor(this.executor)
            .maxInboundMessageSize(config.messageSize)
            .addService(CottonDDLService(this.catalogue))
            .addService(CottonDQLService(this.catalogue, this.engine))
            .addService(CottonDMLService(this.catalogue))
            .let {
                if (config.useTls) {
                    val certFile = config.certFile?.toFile() ?: throw Exception()
                    val privateKeyFile = config.privateKey?.toFile() ?: throw Exception()
                    it.useTransportSecurity(certFile,privateKeyFile)
                } else {
                    it
                }
            }.build()


    /** Companion object with Logger reference. */
    companion object {
        val LOGGER: Logger = LogManager.getLogger(CottontailGrpcServer::class.qualifiedName)
    }

    /**
     * Returns true if this [CottontailGrpcServer] is currently running, and false otherwise.
     */
    val isRunning: Boolean
        get() = !this.server.isShutdown

    /**
     * Starts this instance of [CottontailGrpcServer].
     */
    fun start() {
        this.server.start()
        LOGGER.info("Cottontail DB server is up and running! Hop along...")
    }

    /**
     * Stops this instance of [CottontailGrpcServer].
     */
    fun stop() {
        this.server.shutdown()
        LOGGER.info("Cottontail DB was shut down. Have a binky day!")
    }
}