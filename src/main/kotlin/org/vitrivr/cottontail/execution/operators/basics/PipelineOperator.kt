package org.vitrivr.cottontail.execution.operators.basics

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator] that can be pipelined, i.e., has a parent [Operator] and no materialization of
 * intermediate results is required.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class PipelineOperator(val parent: ProducingOperator, context: ExecutionEngine.ExecutionContext) : ProducingOperator(context) {

    /**
     * True, if this [PipelineOperator] is depleted, i.e., won't return any more [Record]s.
     * Usually depends on the parent [Operator]. Can be overridden.
     */
    override val depleted: Boolean
        get() = this.parent.depleted

    /** [PipelineOperator]s are operational if their parent [Operator] is operational. */
    override val operational: Boolean
        get() = this.parent.operational

    /** Implementation of [Operator.open] */
    final override fun open() {
        check(this.status == OperatorStatus.CREATED) { "Cannot open operator that is in state ${this.status}." }

        /* Call parent.open(). */
        val p = this.parent
        p.open()

        /* Execute preparation. */
        this.prepareOpen()

        /* Update status. */
        this.status = OperatorStatus.OPEN
    }

    /** Implementation of [Operator.next] */
    override fun next(): Record? {
        check(this.status == OperatorStatus.OPEN) { "Cannot call next() on an operator that is in state ${this.status}." }
        return this.getNext(this.parent.next())
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
     * Produces the next [Record] based on the provided input [Record].
     *
     * @param input The input [Record] to operate on.
     * @return  The resulting [Record].
     */
    protected abstract fun getNext(input: Record?): Record?

    /**
     * This method can be used to make necessary preparations, e.g., acquire relevant locks,
     * pre-fetch data etc. prior to query execution. Called by [Operator.open].
     *
     * Can be overridden by an implementing class.
     */
    protected fun prepareOpen() {
        /* No op. */
    }

    /**
     * This method can be used to make necessary preparations prior to closing the operation.
     * Called by [Operator.close].
     *
     * Can be overridden by an implementing class.
     */
    protected fun prepareClose() {
        /* No op. */
    }
}