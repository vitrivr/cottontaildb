package org.vitrivr.cottontail.dbms.execution.operators.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

/**
 * An abstract [Operator.SourceOperator] for execution of DDL statements. Usually processes these
 * statements and returns a status result set.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
abstract class AbstractDataDefinitionOperator(protected val dboName: Name, protected val action: String) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("action"), Types.String, false),
            ColumnDef(Name.ColumnName("dbo"), Types.String, false),
            ColumnDef(Name.ColumnName("duration_ms"), Types.Double, false)
        )
    }

    /** The [ColumnDef] produced by this [AbstractDataDefinitionOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS

    /**
     * Generates and returns a [StandaloneRecord] for the given [duration]
     */
    fun statusRecord(duration: Duration) = StandaloneRecord(0L, this.columns.toTypedArray(), arrayOf(StringValue(this.action), StringValue(dboName.toString()), DoubleValue(duration.inWholeMilliseconds)))
}