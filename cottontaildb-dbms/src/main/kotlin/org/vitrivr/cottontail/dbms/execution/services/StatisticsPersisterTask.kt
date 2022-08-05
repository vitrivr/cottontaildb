package org.vitrivr.cottontail.dbms.execution.services

import org.slf4j.LoggerFactory

import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue

/**
 * A [Runnable] that persist index and column statistics every once in a while.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class StatisticsPersisterTask(private val catalogue: DefaultCatalogue): Runnable {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(StatisticsPersisterTask::class.java)
    }

    /**
     * Persists statistics.
     */
    override fun run() {
        val tx = this.catalogue.environment.beginExclusiveTransaction()
        try {
            val persisted = this.catalogue.indexStatistics.persistInTransaction(tx) && this.catalogue.columnStatistics.persistInTransaction(tx)
            tx.commit()
            if (persisted) {
                LOGGER.info("Successfully persisted up-to-date index & column statistics!")
            } else {
                LOGGER.info("Persisting up-to-date index and/or column statistics was not necessary!")
            }
        } catch (e: Throwable) {
            tx.abort()
            LOGGER.error("Persisting statistics failed due to error: ${e.message}!")
        }
    }
}