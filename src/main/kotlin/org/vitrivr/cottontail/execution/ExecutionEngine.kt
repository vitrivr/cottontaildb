package org.vitrivr.cottontail.execution

import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine.ExecutionContext
import org.vitrivr.cottontail.execution.operators.basics.SinkOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
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

    /**
     * A concrete [ExecutionContext] used for executing a query.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class ExecutionContext {

        /** The [UUID] that identifies this [ExecutionContext]. */
        val uuid: UUID = UUID.randomUUID()

        /** The [ExecutionStatus] of this [ExecutionContext]. */
        @Volatile
        var state: ExecutionStatus = ExecutionStatus.CREATED
            private set

        /** List of [SinkOperator]s that should be executed in this context. */
        private val operators: List<SinkOperator> = mutableListOf()

        /** Map of all [Entity.Tx] that have been created as part of this [ExecutionContext]. */
        private val transactions: MutableMap<Entity, Entity.Tx> = mutableMapOf()

        /** The maximum amount of parallelism allowed by this [ExecutionEngine] instance. */
        val maxThreads
            get() = this@ExecutionEngine.executor.maximumPoolSize

        /** The number of [Thread]s currently available. This is an estimate and may change very quickly. */
        val availableThreads
            get() = this@ExecutionEngine.executor.maximumPoolSize - this@ExecutionEngine.executor.activeCount

        val coroutineDispatcher
            get() = this@ExecutionEngine.dispatcher

        init {
            this@ExecutionEngine.contexts[this.uuid] = this
        }

        /**
         * Adds the given [SinkOperator] to the list of [SinkOperator]s that should be executed by this [ExecutionContext].
         *
         * @param operator The [SinkOperator] to add.
         */
        fun addOperator(operator: SinkOperator) {
            if (this.operators.contains(operator)) {
                throw IllegalArgumentException("Operator $operator cannot be added to list of operators because that operator is already part of that list.")
            }
            (this.operators as MutableList).add(operator)
        }


        /**
         * Requests a new [Entity.Tx] for the given [Entity] and the given [ColumnDef]s. Calling this
         * method will cause the [Entity.Tx] to be registered.
         *
         * @param entity [Entity] to request the [Entity.Tx] for.
         * @param columns [ColumnDef<*>]s to request the [Entity.Tx] for.
         * @param readonly Whether the new [Entity.Tx] should be readonly-
         *
         * @return [Entity.Tx]
         */
        fun requestTransaction(entity: Entity, columns: Array<ColumnDef<*>>, readonly: Boolean): Entity.Tx {
            if (!this.transactions.containsKey(entity)) {
                this.transactions[entity] = entity.Tx(readonly = readonly, columns = columns, tid = this.uuid)
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
                for (operator in this@ExecutionContext.operators) {
                    /* Close operators. */
                    operator.open()

                    /* Execute flow. */
                    val flow = operator.toFlow(this)
                    flow.collect()

                    /* Close operators. */
                    operator.close()
                }
            }

            /* Remove this ExecutionContext. */
            this.state = ExecutionStatus.COMPLETED
            this@ExecutionEngine.contexts.remove(this.uuid)
        }
    }
}