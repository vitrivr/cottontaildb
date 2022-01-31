package org.vitrivr.cottontail.dbms.execution.operators.sort

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder

/**
 * An abstract [Operator.PipelineOperator] used during query execution. Performs sorting on the specified [ColumnDef]s and
 * returns the [Record] in sorted order. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
abstract class AbstractSortOperator(parent: Operator, sortOn: List<Pair<ColumnDef<*>, SortOrder>>) : Operator.PipelineOperator(parent) {

    /** The [AbstractSortOperator] retains the [ColumnDef] of the
     *  input. */
    override val columns: List<ColumnDef<*>>
        get() = this.parent.columns

    /** The [AbstractSortOperator] is always a pipeline breaker. */
    override val breaker: Boolean = true

    /** The [Comparator] used for sorting. */
    protected val comparator: Comparator<Record> = RecordComparator.fromList(sortOn)
}