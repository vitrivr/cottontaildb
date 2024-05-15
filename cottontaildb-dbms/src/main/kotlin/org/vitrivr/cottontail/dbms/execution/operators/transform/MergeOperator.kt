package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.merge
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [MergeOperator] merges the results of multiple incoming operators into a single [Flow].
 *
 * The incoming [Operator]s are executed in parallel, hence order of the [Tuple]s in the
 * outgoing [Flow] may be arbitrary.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */

class MergeOperator(parents: List<Operator>, override val context: QueryContext): Operator.MergingPipelineOperator(parents) {
    /** The columns produced by this [MergeOperator]. */
    override val columns: List<ColumnDef<*>>
        get() = this.parents.first().columns

    /** [MergeOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [MergeOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [MergeOperator]
     */
    override fun toFlow(): Flow<Tuple> = this@MergeOperator.parents.map { it.toFlow() }.merge()
}