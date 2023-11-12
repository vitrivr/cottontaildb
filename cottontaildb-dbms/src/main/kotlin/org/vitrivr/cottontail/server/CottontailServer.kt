package org.vitrivr.cottontail.server

import io.grpc.Server
import io.grpc.ServerBuilder
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.execution.ExecutionManager
import org.vitrivr.cottontail.dbms.execution.services.AutoRebuilderService
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.server.grpc.services.DDLService
import org.vitrivr.cottontail.server.grpc.services.DMLService
import org.vitrivr.cottontail.server.grpc.services.DQLService
import org.vitrivr.cottontail.server.grpc.services.TXNService
import java.io.Closeable
import kotlin.time.ExperimentalTime

/**
 * Main server class for Cottontail DB. This is where all comes together!
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class CottontailServer(config: Config): Closeable {

    /** The [ExecutionManager] used for handling gRPC calls and executing queries. */
    private val executor = ExecutionManager(config)

    /** The [TransactionManager] instance used by this [CottontailServer]. */
    private val manager: TransactionManager = TransactionManager(this.executor, config)

    /** The internal gRPC server; if building that server fails then the [DefaultCatalogue] is closed again! */
    private val grpc: Server

    /* Start server and close catalogue upon failure. */
    init {
        /* Register the different service tasks. */
        val rebuilderService = AutoRebuilderService(this.manager)

        /* Prepare gRPC server itself. */
        this.grpc = ServerBuilder.forPort(config.server.port)
            .executor(this.executor.connectionWorkerPool)
            .addService(DDLService(this.manager, rebuilderService))
            .addService(DMLService(this.manager))
            .addService(DQLService(this.manager))
            .addService(TXNService(this.manager))
            .let {
                if (config.server.useTls) {
                    val certFile = config.server.certFile!!.toFile()
                    val privateKeyFile = config.server.privateKey!!.toFile()
                    it.useTransportSecurity(certFile, privateKeyFile)
                } else {
                    it
                }
            }.build()

        /* Start gRPC server. */
        this.grpc.start()
    }

    /**
     * Returns true if this [CottontailServer] is currently running, and false otherwise.
     */
    @Volatile
    var closed: Boolean = false
        private set

    /**
     * Stops this instance of [CottontailServer].
     */
    override fun close() {
        if (!this.closed) {
            /* Shutdown gRPC server. */
            this.grpc.shutdown()

            /* Closes the TransactionManager. */
            this.manager.close()

            /* Shutdown thread pool executor. */
            this.grpc.awaitTermination()
            this.executor.shutdownAndWait()
        }
    }
}