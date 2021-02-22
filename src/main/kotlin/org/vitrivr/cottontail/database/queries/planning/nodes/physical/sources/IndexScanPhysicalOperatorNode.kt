package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicateHint
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.EntityIndexScanOperator
import org.vitrivr.cottontail.execution.operators.transform.MergeOperator

/**
 * A [IndexScanPhysicalOperatorNode] that represents a predicated lookup using an [AbstractIndex].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class IndexScanPhysicalOperatorNode(val index: Index, val predicate: Predicate) :
    NullaryPhysicalOperatorNode() {
    override val columns: Array<ColumnDef<*>> = this.index.produces
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = this.index.supportsPartitioning
    override val outputSize: Long = this.index.parent.numberOfRows
    override val cost: Cost = this.index.cost(this.predicate)
    override fun copy() = IndexScanPhysicalOperatorNode(this.index, this.predicate)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        return if (this.predicate is KnnPredicate) {
            val hint = this.predicate.hint
            val pMax = tx.availableThreads / 4
            val p = if (hint is KnnPredicateHint.ParallelKnnHint) {
                Integer.max(hint.min, Integer.min(pMax, hint.max))
            } else {
                Integer.min(this.cost.parallelisation(), pMax)
            }

            if (p > 1 && this.canBePartitioned) {
                val partitions = this.partition(p)
                val operators = partitions.map { it.toOperator(tx, ctx) }
                MergeOperator(operators)
            } else {
                EntityIndexScanOperator(this.index, this.predicate.bindValues(ctx))
            }
        } else {
            EntityIndexScanOperator(this.index, this.predicate.bindValues(ctx))
        }
    }
    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        check(this.index.supportsPartitioning) { "Index ${index.name} does not support partitioning!" }
        val partitionSize = Math.floorDiv(this.outputSize, p.toLong())
        return (0 until p).map {
            val start = (it * partitionSize)
            val end = ((it + 1) * partitionSize) - 1
            RangedIndexScanPhysicalOperatorNode(this.index, this.predicate, start until end)
        }
    }

    /**
     * Calculates and returns the digest for this [IndexScanPhysicalOperatorNode].
     *
     * @return Digest for this [IndexScanPhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.predicate.digest()
        result = 31L * result + this.index.hashCode()
        return result
    }
}