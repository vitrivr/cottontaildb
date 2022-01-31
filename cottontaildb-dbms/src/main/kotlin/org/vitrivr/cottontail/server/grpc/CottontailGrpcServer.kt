package org.vitrivr.cottontail.server.grpc

import io.grpc.ServerBuilder
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.execution.TransactionManager
import org.vitrivr.cottontail.server.grpc.services.DDLService
import org.vitrivr.cottontail.server.grpc.services.DMLService
import org.vitrivr.cottontail.server.grpc.services.DQLService
import org.vitrivr.cottontail.server.grpc.services.TXNService
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

/**
 * Main server class for the gRPC endpoint provided by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@ExperimentalTime
class CottontailGrpcServer(val config: Config) {

    /** The [ThreadPoolExecutor] used for handling gRPC calls and executing queries. */
    private val executor = this.config.execution.newExecutor()

    /** The [TransactionManager] used by this [CottontailGrpcServer] instance. */
    private val transactionManager: org.vitrivr.cottontail.dbms.execution.TransactionManager = org.vitrivr.cottontail.dbms.execution.TransactionManager(this.config.execution.transactionTableSize, this.config.execution.transactionHistorySize)

    /** The [DefaultCatalogue] instance used by this [CottontailGrpcServer]. */
    val catalogue = DefaultCatalogue(this.config)

    /** The internal gRPC server; if building that server fails then the [DefaultCatalogue] is closed again! */
    private val server = ServerBuilder.forPort(this.config.server.port)
        .executor(this.executor)
        .addService(DDLService(this.catalogue, this.transactionManager))
        .addService(DMLService(this.catalogue, this.transactionManager))
        .addService(DQLService(this.catalogue, this.transactionManager))
        .addService(TXNService(this.catalogue, this.transactionManager))
        .let {
            if (this.config.server.useTls) {
                val certFile = this.config.server.certFile!!.toFile()
                val privateKeyFile = this.config.server.privateKey!!.toFile()
                it.useTransportSecurity(certFile, privateKeyFile)
            } else {
                it
            }
        }.build()


    /* Start server and close catalogue upon failure. */
    init {
        try {
            this.server.start()
        } catch (e: Throwable) {
            this.catalogue.close()
        }
    }

    /**
     * Returns true if this [CottontailGrpcServer] is currently running, and false otherwise.
     */
    val isRunning: Boolean
        get() = !this.server.isShutdown

    /**
     * Stops this instance of [CottontailGrpcServer].
     */
    fun stop() {
        /* Shutdown gRPC server. */
        this.server.shutdown()
        this.server.awaitTermination()

        /* Shutdown thread pool executor. */
        this.executor.shutdown()
        this.executor.awaitTermination(5000, TimeUnit.MILLISECONDS)

        /* Close catalogue. */
        this.catalogue.close()
    }
}