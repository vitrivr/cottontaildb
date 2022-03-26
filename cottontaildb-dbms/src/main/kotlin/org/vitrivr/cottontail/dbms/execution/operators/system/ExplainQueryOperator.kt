package org.vitrivr.cottontail.dbms.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.BinaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode

/**
 * An [Operator.SourceOperator] used during query execution. Used to explain queries
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class ExplainQueryOperator(private val candidates: Collection<OperatorNode.Physical>, private val costPolicy: CostPolicy) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("path"), Types.String, false),
            ColumnDef(Name.ColumnName("name"), Types.String, false),
            ColumnDef(Name.ColumnName("output_size"), Types.Long, false),
            ColumnDef(Name.ColumnName("cost_cpu"), Types.Float, false),
            ColumnDef(Name.ColumnName("cost_io"), Types.Float, false),
            ColumnDef(Name.ColumnName("cost_memory"), Types.Float, false),
            ColumnDef(Name.ColumnName("cost_accuracy"), Types.Float, false),
            ColumnDef(Name.ColumnName("traits"), Types.String, false),
            ColumnDef(Name.ColumnName("comment"), Types.String, false)
        )
    }

    override val columns: List<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val candidate = this.candidates.minByOrNull { this@ExplainQueryOperator.costPolicy.toScore(it.totalCost) }!!
        val columns = this.columns.toTypedArray()
        val values = Array<Value?>(this@ExplainQueryOperator.columns.size) { null }
        return flow {
            val plan = enumerate(emptyArray(), candidate)
            var row = 0L
            for (p in plan) {
                val node = p.second
                values[0] = StringValue(p.first)
                values[1] = StringValue(node.javaClass.simpleName)
                values[2] = LongValue(node.outputSize)
                values[3] = FloatValue(node.cost.cpu)
                values[4] = FloatValue(node.cost.io)
                values[5] = FloatValue(node.cost.memory)
                values[6] = FloatValue(node.cost.accuracy)
                values[7] = StringValue(node.traits.map { it.value.toString() }.joinToString(","))
                values[8] = StringValue(node.toString())
                emit(StandaloneRecord(row++, columns, values))
            }
        }
    }


    /**
     * Enumerates a query plan and returns a sorted list of [OperatorNode.Physical] with their exact path.
     *
     * @param path An [Array] tracking the current depth of the execution plan.
     * @param nodes The [OperatorNode.Physical] to explain.
     */
    private fun enumerate(path: Array<Int> = emptyArray(), vararg nodes: OperatorNode.Physical): List<Pair<String,OperatorNode.Physical>> {
        val list = mutableListOf<Pair<String, OperatorNode.Physical>>()
        for ((index, node) in nodes.withIndex()) {
            val newPath = (path + index)
            when (node) {
                is NullaryPhysicalOperatorNode -> list += Pair(newPath.joinToString("."), node)
                is UnaryPhysicalOperatorNode -> list += this.enumerate(newPath, node.input ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $node). This is a programmer's error!"))
                is BinaryPhysicalOperatorNode -> list += this.enumerate(
                    newPath,
                    node.left ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $node). This is a programmer's error!"),
                    node.right ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $node). This is a programmer's error!")
                )
                is NAryPhysicalOperatorNode -> list += this.enumerate(newPath, *node.inputs.toTypedArray())
            }
        }
        return list
    }
}