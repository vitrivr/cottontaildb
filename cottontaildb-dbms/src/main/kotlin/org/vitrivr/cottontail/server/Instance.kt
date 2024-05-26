package org.vitrivr.cottontail.server

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.execution.ExecutionManager
import org.vitrivr.cottontail.dbms.execution.services.AutoRebuilderService
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.statistics.StatisticsManager
import java.io.Closeable

/**
 * An [Instance] of a Cottontail DBMS. An [Instance] is a single, self-contained instance of Cottontail DBMS that contains a [Catalogue] and a [StatisticsManager].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Instance(val config: Config): Closeable {

    companion object {
        /** [Logger] instance used by [DefaultCatalogue]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(Instance::class.java)
    }

    /** The [DefaultCatalogue] used by this [Instance]. */
    val catalogue = DefaultCatalogue(config)

    /** The [ExecutionManager] used for handling gRPC calls and executing queries. */
    internal val executor = ExecutionManager(config)

    /** The [TransactionManager] used by this [Instance]. */
    internal val transactions = TransactionManager(this)

    /** The [StatisticsManager] used by this [Instance]. */
    val statistics = StatisticsManager(this)

    /** Register the different service tasks. */
    val rebuilderService = AutoRebuilderService(this)

    init {
        /* Register the statistics manager with the transaction manager. */
        this.transactions.register(this.statistics)
    }

    /**
     * Closes the [Instance] and all objects contained within.
     */
    override fun close() {
        try {
            this.executor.shutdownAndWait()
        } catch (e: Throwable) {

        }

        try {
            this.statistics.close()
        } catch (e: Throwable) {

        }


        try {
            this.catalogue.close()
        } catch (e: Throwable) {

        }
    }
}