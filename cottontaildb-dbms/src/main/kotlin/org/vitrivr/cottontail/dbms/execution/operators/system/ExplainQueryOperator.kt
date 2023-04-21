package org.vitrivr.cottontail.dbms.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.MissingRecord
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.*
import java.util.*

/**
 * An [Operator.SourceOperator] used during query execution. Used to explain queries
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class ExplainQueryOperator(private val candidates: Map<GroupId,List<Pair<OperatorNode.Physical,Float>>>, override val context: QueryContext) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("groupId"), Types.Int, false),
            ColumnDef(Name.ColumnName("rank"), Types.Int, false),
            ColumnDef(Name.ColumnName("score"), Types.Float, false),
            ColumnDef(Name.ColumnName("position"), Types.Int, false),
            ColumnDef(Name.ColumnName("name"), Types.String, false),
            ColumnDef(Name.ColumnName("output_size"), Types.Long, false),
            ColumnDef(Name.ColumnName("cost_cpu"), Types.Float, false),
            ColumnDef(Name.ColumnName("cost_io"), Types.Float, false),
            ColumnDef(Name.ColumnName("cost_memory"), Types.Float, false),
            ColumnDef(Name.ColumnName("cost_accuracy"), Types.Float, false),
            ColumnDef(Name.ColumnName("digest"), Types.Long, false),
            ColumnDef(Name.ColumnName("designation"), Types.String, false)
        )
    }

    override val columns: List<ColumnDef<*>> = COLUMNS

    override fun toFlow(): Flow<Record> = flow {
        with(this@ExplainQueryOperator.context.bindings) {
            with(MissingRecord) {
                val columns = this@ExplainQueryOperator.columns.toTypedArray()
                for ((groupId, plans) in this@ExplainQueryOperator.candidates) {
                    var rank = 1
                    for ((plan, score) in plans) {
                        val enumerated = enumerate(LinkedList(), emptyArray(), plan)
                        var row = 0L
                        var position = 1
                        for (p in enumerated) {
                            val node = p.second
                            val values = arrayOf<Value?>(
                                IntValue(groupId),
                                IntValue(rank),
                                FloatValue(score),
                                IntValue(position),
                                StringValue(p.first),
                                LongValue(node.outputSize),
                                FloatValue(node.cost.cpu),
                                FloatValue(node.cost.io),
                                FloatValue(node.cost.memory),
                                FloatValue(node.cost.accuracy),
                                LongValue(node.totalDigest()),
                                StringValue(node.toString())
                            )
                            emit(StandaloneRecord(row++, columns, values))
                            position += 1
                        }
                        rank += 1
                    }
                }
            }
        }
    }


    /**
     * Enumerates a query plan and returns a sorted list of [OperatorNode.Physical] with their exact path.
     *
     * @param path An [Array] tracking the current depth of the execution plan.
     * @param nodes The [OperatorNode.Physical] to explain.
     */
    private fun enumerate(list: MutableList<Pair<String, OperatorNode.Physical>>, path: Array<Int>, vararg nodes: OperatorNode.Physical): List<Pair<String, OperatorNode.Physical>> {
        for ((index, node) in nodes.withIndex()) {
            val newPath = (path + index)
            list.add(Pair(newPath.joinToString("."), node))
            when (node) {
                is NullaryPhysicalOperatorNode -> { /* No op. */ }
                is UnaryPhysicalOperatorNode -> this.enumerate(list, newPath, node.input)
                is BinaryPhysicalOperatorNode -> this.enumerate(list, newPath, node.left, node.right)
                is NAryPhysicalOperatorNode -> this.enumerate(list, newPath, *node.inputs.toTypedArray())
            }
        }
        return list
    }
}