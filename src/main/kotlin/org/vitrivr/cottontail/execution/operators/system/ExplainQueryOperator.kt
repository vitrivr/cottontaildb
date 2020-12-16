package org.vitrivr.cottontail.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

import org.vitrivr.cottontail.database.queries.planning.nodes.physical.PhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.SourceOperator] used during query execution. Used to explain queries
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ExplainQueryOperator(val candidates: Collection<PhysicalNodeExpression>) : Operator.SourceOperator() {
    override val columns: Array<ColumnDef<*>>
        get() = arrayOf(
                ColumnDef.withAttributes(Name.ColumnName("path"), "STRING", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("name"), "STRING", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("output_size"), "INTEGER", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("cost_cpu"), "STRING", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("cost_io"), "INTEGER", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("cost_memory"), "STRING", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("partitionable"), "BOOLEAN", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("comment"), "STRING", -1, true)

        )

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val candidate = this.candidates.sortedBy { it.totalCost }.first()
        return flow {
            val plan = enumerate(nodes = listOf(candidate))
            var row = 0L
            val array = Array<Value?>(this@ExplainQueryOperator.columns.size) { null }
            for (p in plan) {
                array[0] = StringValue(p.first)
                array[1] = StringValue(p.second.javaClass.simpleName)
                array[2] = LongValue(p.second.outputSize)
                array[3] = FloatValue(p.second.cost.cpu)
                array[4] = FloatValue(p.second.cost.io)
                array[5] = FloatValue(p.second.cost.memory)
                array[6] = BooleanValue(p.second.canBePartitioned)
                array[7] = null
                emit(StandaloneRecord(row++, this@ExplainQueryOperator.columns, array))
            }
        }
    }


    /**
     * Enumerates a query plan and returns a sorted list of [PhysicalNodeExpression] with their exact path.
     */
    fun enumerate(path: Array<Int> = emptyArray(), nodes: List<PhysicalNodeExpression>): List<Pair<String, PhysicalNodeExpression>> {
        val list = mutableListOf<Pair<String, PhysicalNodeExpression>>()
        for ((index, node) in nodes.withIndex()) {
            val newPath = (path + index)
            if (node.inputs.size > 0) {
                list += this.enumerate(newPath, node.inputs as List<PhysicalNodeExpression>)
            }
            list.add(Pair(newPath.joinToString("."), node))
        }
        return list
    }
}