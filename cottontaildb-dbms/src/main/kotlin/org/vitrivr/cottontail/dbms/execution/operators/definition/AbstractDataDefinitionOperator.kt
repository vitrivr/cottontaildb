package org.vitrivr.cottontail.dbms.execution.operators.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An abstract [Operator.SourceOperator] for execution of DDL statements. Usually processes these
 * statements and returns a status result set.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class AbstractDataDefinitionOperator(protected val dboName: Name, protected val action: String) : Operator.SourceOperator() {
    /** The [ColumnDef] produced by this [AbstractDataDefinitionOperator]. */
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_STATUS_COLUMNS

    /**
     * Generates and returns a [StandaloneRecord] for the given [duration_ms]
     */
    fun statusRecord(duration_ms: Long) = StandaloneRecord(0L, this.columns.toTypedArray(), arrayOf(StringValue(this.action), StringValue(dboName.toString()), LongValue(duration_ms)))
}