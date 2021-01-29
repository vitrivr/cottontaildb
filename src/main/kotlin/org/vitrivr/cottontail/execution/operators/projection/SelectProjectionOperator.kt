package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 * An [Operator.PipelineOperator] used during query execution. It generates new [Record]s for
 * each incoming [Record] and removes / renames field according to the [fields] definition provided.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class SelectProjectionOperator(
    parent: Operator,
    private val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>
) : Operator.PipelineOperator(parent) {

    /** True if names should be flattened, i.e., prefixes should be removed. */
    private val flattenNames =
        this.fields.all { it.first.schema() == this.fields.first().first.schema() }

    /** List of expanded names, i.e., wildcards are made explicit. Important: Order as defined in [fields] is maintained! */
    private val expandedNames = this.fields.flatMap { field ->
        if (field.first.wildcard) {
            this.parent.columns.filter { field.first.matches(it.name) }
                .map { Pair(it.name, field.second) }
        } else {
            listOf(field)
        }
    }

    /** Mapping from input [ColumnDef] to output [Name.ColumnName]. */
    private val mapping: List<Pair<ColumnDef<*>, Name.ColumnName>> = this.expandedNames.map {
        val original = it.first
        val alias = it.second
        val column =
            this.parent.columns.find { it.name == original } ?: throw IllegalStateException("")
        if (this.flattenNames) {
            column to (alias ?: Name.ColumnName(column.name.simple))
        } else {
            column to (alias ?: column.name)
        }
    }

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = this.mapping.map {
        ColumnDef.withAttributes(
            it.second,
            it.first.type.name,
            it.first.logicalSize,
            it.first.nullable
        )
    }.toTypedArray()

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return this.parent.toFlow(context).map { r ->
            StandaloneRecord(
                r.tupleId,
                this.columns,
                this.mapping.map { r[it.first] }.toTypedArray()
            )
        }
    }
}