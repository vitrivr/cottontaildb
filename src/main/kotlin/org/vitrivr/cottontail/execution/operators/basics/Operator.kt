package org.vitrivr.cottontail.execution.operators.basics

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator] used during query execution.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class Operator(val context: ExecutionEngine.ExecutionContext) : AutoCloseable {
    /** Status of the [Operator]. */
    var status: OperatorStatus = OperatorStatus.CREATED
        protected set

    /** True if this [Operator] is unable to produce more [Record]s. */
    abstract val depleted: Boolean

    /**
     * True if the pipeline up and until this [Operator] is operational, i.e., can be processed and
     * is expected to produce data.
     */
    abstract val operational: Boolean

    /** The list of [ColumnDef]s produced by this [Operator]. */
    abstract val columns: Array<ColumnDef<*>>

    /**
     * Opens this [Operator], i.e., signals the impending start of query execution. If there
     * is a parent [Operator], then this call is usually propagated.
     */
    abstract fun open()
}