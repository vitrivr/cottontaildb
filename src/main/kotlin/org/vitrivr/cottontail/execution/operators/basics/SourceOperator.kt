package org.vitrivr.cottontail.execution.operators.basics

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator] that acts as a source, i.e., thus has no parent [Operator].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class SourceOperator(context: ExecutionEngine.ExecutionContext) : ProducingOperator(context) {

    final override fun open() {
        check(this.status == OperatorStatus.CREATED) { "Cannot open operator that is in state ${this.status}." }

        /* Execute preparation. */
        this.prepareOpen()

        /* Update status. */
        this.status = OperatorStatus.OPEN
    }

    /** Implementation of [Operator.next] */
    final override fun next(): Record? {
        check(this.status == OperatorStatus.OPEN) { "Cannot call next() on an operator that is in state ${this.status}." }
        return this.getNext()
    }

    /** Implementation of [Operator.close] */
    final override fun close() {
        check(this.status == OperatorStatus.OPEN) { "Cannot close operator that is state ${this.status}." }

        /* Execute preparation. */
        this.prepareClose()

        /* Update status. */
        this.status = OperatorStatus.CLOSED
    }

    /**
     * Produces the next [Record] and returns it.
     *
     * @return The resulting [Record].
     */
    protected abstract fun getNext(): Record?

    /**
     * This method can be used to make necessary preparations, e.g., acquire relevant locks,
     * pre-fetch data etc. prior to query execution. Called by [Operator.open].
     */
    protected abstract fun prepareOpen()

    /**
     * This method can be used to make necessary preparations prior to closing the operation.
     * Called by [Operator.close].
     */
    protected abstract fun prepareClose()
}