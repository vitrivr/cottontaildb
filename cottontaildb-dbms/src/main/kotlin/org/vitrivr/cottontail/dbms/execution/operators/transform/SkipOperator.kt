package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.basics.drop
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. Skips incoming [Record]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class SkipOperator(parent: Operator, private val skip: Long, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Columns returned by [SkipOperator] depend on the parent [Operator]. */
    override val columns: List<ColumnDef<*>>
        get() = this.parent.columns

    /** [SkipOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SkipOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [SkipOperator]
     */
    override fun toFlow(): Flow<Record>
        = this.parent.toFlow().drop(this.skip)
}