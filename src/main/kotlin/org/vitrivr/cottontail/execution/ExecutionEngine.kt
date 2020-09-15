package org.vitrivr.cottontail.execution

import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine.ExecutionContext
import org.vitrivr.cottontail.execution.operators.basics.SinkOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import java.util.*
import java.util.concurrent.*
import java.util.function.Consumer

/**
 * The default [ExecutionEngine] for Cottontail DB. It hosts all the necessary facilities to create
 * and execute query [ExecutionContext]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class ExecutionEngine(config: ExecutionConfig) {

    /** The [ThreadPoolExecutor] used for executing queries. */
    private val executor = ThreadPoolExecutor(config.coreThreads, config.maxThreads, config.keepAliveMs, TimeUnit.MILLISECONDS, ArrayBlockingQueue(config.queueSize))

    /** The maximum amount of parallelism allowed by this [ExecutionEngine] instance. */
    val maxThreads
        get() = this.executor.maximumPoolSize

    /** The number of [Thread]s currently available. This is an estimate and may change very quickly. */
    val availableThreads
        get() = this.executor.maximumPoolSize - this.executor.activeCount

    /** Set of [ExecutionContext]s that are currently PENDING or RUNNING. */
    val contexts = ConcurrentHashMap<UUID, ExecutionContext>()

    /**
     * A concrete [ExecutionContext] used for executing a query.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class ExecutionContext : Runnable {

        /** The [UUID] that identifies this [ExecutionContext]. */
        val uuid: UUID = UUID.randomUUID()

        /** The [ExecutionStatus] of this [ExecutionContext]. */
        @Volatile
        var state: ExecutionStatus = ExecutionStatus.CREATED
            private set

        /** List of [SinkOperator]s that should be executed in this context. */
        val operators: List<SinkOperator> = mutableListOf()

        /** Map of all [Entity.Tx] that have been created as part of this [ExecutionContext]. */
        val transactions: MutableMap<Entity, Entity.Tx> = mutableMapOf()

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
            if (!operator.operational) {
                throw IllegalArgumentException("Operator $operator cannot be added to list of operators because that operator is not operational.")
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
         * Schedules this [ExecutionContext] for execution. Can only be invoked once on a given instance of [ExecutionContext]
         *
         * @throws IllegalStateException If [ExecutionContext] has been scheduled already.
         */
        @Synchronized
        fun schedule() {
            check(this.state == ExecutionStatus.CREATED) { "Cannot schedule ExecutionContext ${this.uuid} because it is in state ${this.state}." }

            if (!this.operators.all { it.operational }) {
                TODO()
            }

            this@ExecutionEngine.executor.execute(this)
            this.state = ExecutionStatus.SCHEDULED
        }

        /**
         * Executes a branch represented by a [Callable]. Waits for the [Callable] to complete.
         *
         * @param callable The [Callable] that should be executed.
         * @return The [Recordset] produced by the [Callable]
         */
        fun executeBranch(branch: Callable<Recordset>): Future<Recordset> = this@ExecutionEngine.executor.submit(branch)

        /**
         * Executes a branches represented by a list of [Callable]s. Waits for all [Callable]s
         * to complete.
         *
         * @param callables The [Callable]s that should be executed.
         * @return The [Recordset]s produced by the [Callable]s
         */
        fun executeBranches(branches: List<Callable<Recordset>>): List<Future<Recordset>> = branches.map { this@ExecutionEngine.executor.submit(it) }

        /**
         * Executes this [ExecutionContext] and pushes all [Record]s into the provided [Consumer].
         */
        @Synchronized
        override fun run() {
            /* Check and update state. */
            check(this.state == ExecutionStatus.SCHEDULED) { "Cannot run ExecutionContext ${this.uuid} because it is in state ${this.state}." }
            this.state = ExecutionStatus.RUNNING

            /* Process operators one by one. */
            for (operator in this.operators) {
                operator.open()

                /* Execute the operator. */
                while (!operator.depleted) {
                    val next = operator.process()
                }

                /* Close operators. */
                operator.close()
            }

            /* Update state and remove context. */
            this.state = ExecutionStatus.COMPLETED
            this@ExecutionEngine.contexts.remove(this.uuid)
        }
    }

}