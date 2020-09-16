package org.vitrivr.cottontail.execution.operators.basics

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import java.util.concurrent.Callable

/**
 * An [Operator] that can be pipelined and has multiple, incoming parent [Operator]s that must
 * be materialized before processing can continue.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class MergingPipelineBreaker(val parents: List<ProducingOperator>, context: ExecutionEngine.ExecutionContext) : ProducingOperator(context) {

    /** Cached [Recordset]s that contain materialized data for each parent [Operator]. */
    protected val caches: MutableList<Recordset> = mutableListOf()

    /** [MergingPipelineBreaker]s are operational if all their parent [Operator]s are operational. */
    override val operational: Boolean
        get() = this.parents.all { it.operational }

    /** Implementation of [Operator.open] */
    override fun open() {
        check(this.status == OperatorStatus.CREATED) { "Cannot open operator that is in state ${this.status}." }

        /* Call parents. */
        this.parents.forEach { it.open() }

        /* Execute preparation. */
        this.prepareOpen()

        /* Update status. */
        this.status = OperatorStatus.OPEN
    }

    /** Implementation of [ProducingOperator.next] */
    override fun next(): Record? {
        check(this.status == OperatorStatus.OPEN) { "Cannot call next() on an operator that is in state ${this.status}." }

        /* Checks if cache is null and triggers execution of incoming operators, if true. */
        if (this.caches.isEmpty()) {
            val branches = this.incomingOperators()
            val future = this.context.executeBranches(branches)
            future.forEach { this.caches.add(it.get()) }
        }

        /* Returns the next record. */
        return this.getNext()
    }

    /**
     * Produces the next [Record] and returns it.
     *
     * @return The next [Record]
     */
    protected abstract fun getNext(): Record?

    /** Implementation of [Operator.close] */
    override fun close() {
        check(this.status == OperatorStatus.OPEN) { "Cannot close operator that is state ${this.status}." }

        /* Call parents. */
        this.parents.forEach { it.close() }

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
        this.caches.clear()
    }

    /**
     * Executes the incoming (parent) [Operator] and waits for its completion. Returns
     * a [Recordset] that materializes the data produced by the incoming [Operator]
     *
     * @return [Recordset] of materialized data.
     */
    abstract fun incomingOperators(): List<Callable<Recordset>>
}