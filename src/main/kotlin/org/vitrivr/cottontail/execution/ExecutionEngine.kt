package org.vitrivr.cottontail.execution

import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine.ExecutionContext
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.execution.exceptions.OperatorExecutionException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * The default [ExecutionEngine] for Cottontail DB. It hosts all the necessary facilities to create
 * and execute query [ExecutionContext]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class ExecutionEngine(config: ExecutionConfig) {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(ExecutionEngine::class.java)
    }

    /** The [ThreadPoolExecutor] used for executing queries. */
    private val executor = ThreadPoolExecutor(
            config.coreThreads,
            config.maxThreads,
            config.keepAliveMs,
            TimeUnit.MILLISECONDS,
            ArrayBlockingQueue(config.queueSize)
    )

    /** The [ExecutorCoroutineDispatcher] used for executing queries. */
    private val dispatcher = this.executor.asCoroutineDispatcher()

    /** Set of [ExecutionContext]s that are currently PENDING or RUNNING. */
    val contexts = ConcurrentHashMap<UUID, ExecutionContext>()

    /** The number of [Thread]s currently available. This is an estimate and may change very quickly. */
    val availableThreads
        get() = this@ExecutionEngine.executor.maximumPoolSize - this@ExecutionEngine.executor.activeCount

    /**
     * A concrete [ExecutionContext] used for executing a query.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    inner class ExecutionContext(private val operator: Operator.SinkOperator) {

        /** The [UUID] that identifies this [ExecutionContext]. */
        val uuid: UUID = UUID.randomUUID()

        /** The [ExecutionStatus] of this [ExecutionContext]. */
        @Volatile
        var state: ExecutionStatus = ExecutionStatus.CREATED
            private set

        /** Map of all [Entity.Tx] that have been created as part of this [ExecutionContext]. */
        private val transactions: MutableMap<Entity, Entity.Tx> = mutableMapOf()

        /** The maximum amount of parallelism allowed by this [ExecutionEngine] instance. */
        val maxThreads
            get() = this@ExecutionEngine.executor.maximumPoolSize

        /** The number of [Thread]s currently available. This is an estimate and may change very quickly. */
        val availableThreads
            get() = this@ExecutionEngine.executor.maximumPoolSize - this@ExecutionEngine.executor.activeCount

        /** */
        val coroutineDispatcher
            get() = this@ExecutionEngine.dispatcher

        init {
            this@ExecutionEngine.contexts[this.uuid] = this
        }

        /**
         * Returns the [Entity.Tx] for the provided [Entity]. If such an [Entity.Tx] doesn't
         * exist, an [ExecutionException] is thrown.
         *
         * @param entity [Entity] to return the [Entity.Tx] for.
         * @return entity [Entity.Tx]
         */
        fun getTx(entity: Entity, readonly: Boolean = true): Entity.Tx {
            if (!this.transactions.containsKey(entity)) {
                this.transactions[entity] = entity.Tx(readonly = readonly, tid = this.uuid)
            } else if (!readonly && this.transactions[entity]!!.readonly) {
                this.transactions[entity]!!.close() /* Upgrade transaction. */
                this.transactions[entity] = entity.Tx(readonly = readonly, tid = this.uuid)
            }
            return this.transactions[entity]!!
        }

        /**
         * Executes this [ExecutionContext] in the [dispatcher] of the enclosing [ExecutionEngine].
         */
        @Synchronized
        fun execute() {
            check(this.state == ExecutionStatus.CREATED) { "Cannot schedule ExecutionContext ${this.uuid} because it is in state ${this.state}." }

            /* Update state and execute. */
            this.state = ExecutionStatus.RUNNING
            runBlocking(this@ExecutionEngine.dispatcher) {
                /* Execute flow. */
                try {
                    this@ExecutionContext.operator.toFlow(this@ExecutionContext).collect()
                } catch (e: OperatorExecutionException) {
                    LOGGER.error("Unhandled exception during query execution: ${e.stackTraceToString()}")
                    throw e
                } catch (e: Throwable) {
                    LOGGER.error("Unhandled exception during query execution: ${e.stackTraceToString()}")
                    throw ExecutionException("Unhandled exception during query execution: ${e.message}.")
                } finally {
                    /* Close all transactions. */
                    this@ExecutionContext.transactions.forEach { (_, tx) ->
                        tx.close()
                    }
                }
            }

            /* Remove this ExecutionContext. */
            this.state = ExecutionStatus.COMPLETED
            this@ExecutionEngine.contexts.remove(this.uuid)
        }
    }
}