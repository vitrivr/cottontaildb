package org.vitrivr.cottontail.execution.operators.basics

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.transform.LimitOperator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import java.util.concurrent.Callable

/**
 * An [Operator] that can be pipelined and has a single parent [Operator] that must be materialized,
 * before processing can continue.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class PipelineBreaker(val parent: ProducingOperator, context: ExecutionEngine.ExecutionContext) : ProducingOperator(context) {
    /** [PipelineBreaker]s are operational if their parent [Operator] is operational. */
    override val operational: Boolean
        get() = this.parent.operational

    /** True, if this [LimitOperator] is depleted, i.e., won't return any more [Record]s. */
    override val depleted: Boolean
        get() = this.nextIndex >= (this.cache?.rowCount ?: Long.MAX_VALUE)


    /** Cached [Recordset] that contains materialized data. */
    protected var cache: Recordset? = null

    /** Number of [Record]s returned by this [PipelineBreaker]. */
    protected open val size: Long
        get() = (this.cache?.rowCount ?: 0L)

    /** The index of the next [Record]. */
    private var nextIndex = 0L

    /** Implementation of [Operator.open] */
    final override fun open() {
        check(this.status == OperatorStatus.CREATED) { "Cannot open operator that is in state ${this.status}." }

        /* Call parent. */
        this.parent.open()

        /* Execute preparation. */
        this.prepareOpen()

        /* Update status. */
        this.status = OperatorStatus.OPEN
    }

    /** Implementation of [Operator.next] */
    final override fun next(): Record? {
        check(this.status == OperatorStatus.OPEN) { "Cannot call next() on an operator that is in state ${this.status}." }

        /* Checks if cache is null and triggers execution of incoming operators, if true. */
        if (this.cache == null) {
            val branch = this.incomingOperator()
            val future = this.context.executeBranch(branch)
            this.cache = future.get()
        }

        /* Returns the next record. */
        return this.cache!![this.nextIndex++]
    }

    /** Implementation of [Operator.close] */
    final override fun close() {
        check(this.status == OperatorStatus.OPEN) { "Cannot close operator that is state ${this.status}." }

        /* Propagate to parent. */
        this.parent.close()

        /* Execute preparation. */
        this.prepareClose()

        /* Update status. */
        this.status = OperatorStatus.CLOSED
    }

    /**
     * This method can be used to make necessary preparations, e.g., acquire relevant locks,
     * pre-fetch data etc. prior to query execution. Called by [Operator.open].
     *
     * Can be overridden by an implementing class.
     */
    open fun prepareOpen() {
        /* No Op. */
    }

    /**
     * This method can be used to make necessary preparations prior to closing the operation.
     * Called by [Operator.close]. Nulls the cached [Recordset]
     *
     * Can be overridden by an implementing class.
     */
    open fun prepareClose() {
        this.cache = null
    }

    /**
     * Returns a [Callable] that represents the incoming (parent) [Operator]. That [Callable]
     * should produce [Recordset] that materializes the data produced by the incoming [Operator]
     *
     * @return [Callable] that produces a [Recordset] of materialized data.
     */
    abstract fun incomingOperator(): Callable<Recordset>
}