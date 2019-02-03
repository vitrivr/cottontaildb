package ch.unibas.dmi.dbis.cottontail.server.grpc

import ch.unibas.dmi.dbis.cottontail.config.ServerConfig
import ch.unibas.dmi.dbis.cottontail.server.grpc.services.CottonDDLService
import ch.unibas.dmi.dbis.cottontail.server.grpc.services.CottonDMLService
import ch.unibas.dmi.dbis.cottontail.server.grpc.services.CottonDQLService

import io.grpc.ServerBuilder

/**
 * Main server class for the GRPC endpoint provided by Cottotail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class CottontailGrpcServer(private val config: ServerConfig) {

    /** Reference to the GRPC server. */
    private val server = ServerBuilder.forPort(this.config.port)
            .maxInboundMessageSize(this.config.messageSize)
            .addService(CottonDDLService())
            .addService(CottonDQLService())
            .addService(CottonDMLService())
            .let {
                if (this.config.useTls) {
                    val certFile = this.config.certFile?.toFile() ?: throw Exception()
                    val privateKeyFile = this.config.privateKey?.toFile() ?: throw Exception()
                    it.useTransportSecurity(certFile,privateKeyFile)
                } else {
                    it
                }
            }.build()

    /**
     * Returns true if this [CottontailGrpcServer] is currently running, and false otherwise.
     */
    val isRunning: Boolean
        get() = !this.server.isShutdown

    /**
     * Starts this instance of [CottontailGrpcServer].
     */
    fun start() = this.server.start()

    /**
     * Stops this instance of [CottontailGrpcServer].
     */
    fun stop() = this.server.shutdown()
}