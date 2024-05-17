package org.vitrivr.cottontail.test

import io.grpc.ServerBuilder
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.server.CottontailServer
import org.vitrivr.cottontail.server.Instance
import org.vitrivr.cottontail.server.grpc.services.DDLService
import org.vitrivr.cottontail.server.grpc.services.DMLService
import org.vitrivr.cottontail.server.grpc.services.DQLService
import org.vitrivr.cottontail.server.grpc.services.TXNService
import kotlin.time.ExperimentalTime

/**
 * Server class for the gRPC endpoint provided by Cottontail DB used in unit tests.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
@ExperimentalTime
class EmbeddedCottontailGrpcServer(config: Config) {
    /** The [Instance] instance used by this [EmbeddedCottontailGrpcServer]. */
    private val instance = Instance(config)


    /** The internal gRPC server; if building that server fails then the [DefaultCatalogue] is closed again! */
    private val grpc = ServerBuilder.forPort(config.server.port)
        .executor(this.instance.executor.connectionWorkerPool)
        .addService(DDLService(this.instance))
        .addService(DMLService(this.instance))
        .addService(DQLService(this.instance))
        .addService(TXNService(this.instance))
        .let {
            if (config.server.useTls) {
                val certFile = config.server.certFile!!.toFile()
                val privateKeyFile = config.server.privateKey!!.toFile()
                it.useTransportSecurity(certFile, privateKeyFile)
            } else {
                it
            }
        }.build()


    /* Start server and close catalogue upon failure. */
    init {
        try {
            this.grpc.start()
        } catch (e: Throwable) {
            this.instance.close()
            throw e
        }
    }

    /**
     * Returns true if this [CottontailServer] is currently running, and false otherwise.
     */
    @Volatile
    var isRunning: Boolean = true
        private set

    /**
     * Stops this instance of [CottontailServer].
     */
    fun shutdownAndWait() {
        if (this.isRunning) {
            /* Shutdown gRPC server. */
            this.grpc.shutdown().awaitTermination()

            /* Close catalogue. */
            this.instance.close()

            /* Update flag. */
            this.isRunning = false
        }
    }
}