package org.vitrivr.cottontail.dbms.queries.operators.physical.sources

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sources.IndexIntersectionScanOperator
import org.vitrivr.cottontail.dbms.execution.operators.sources.IndexScanOperator
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * A [IndexIntersectionScanPhysicalOperatorNode] that represents an intersections scan predicated lookup using a selection of [AbstractIndex]es.
 *
 * @author Ralph Gasser
 * @version 3.1.0
 */
class IndexIntersectionScanPhysicalOperatorNode(override val groupId: Int, override val columns: List<Binding.Column>, val indexes: List<Pair<IndexTx, Predicate>>) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "ScanAndIntersectIndex"
    }

    /** The name of this [IndexScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** [ValueStatistics] are taken from the underlying [Entity]. The query planner uses statistics for [Cost] estimation. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>, ValueStatistics<*>>()

    /** Cost estimation for [IndexIntersectionScanOperator]s is delegated to the [Index]es. */
    override val cost: Cost by lazy {
        val indexCosts = this.indexes.map { it.first.costFor(it.second) }.reduce { acc, cost -> acc + cost }
        val counts = this.outputSize
        indexCosts + Cost.MEMORY_ACCESS + Cost(memory = (Long.SIZE_BYTES * counts).toFloat())
    }

    /** The estimated output size of this [IndexScanPhysicalOperatorNode]. */
    override val outputSize: Long by lazy {
        this.indexes.minOfOrNull { it.first.countFor(it.second) } ?: 0L
    }

    /** Returns [Map] of [Trait]s for this [IndexScanOperator], which is derived directly from the [Index]*/
    override val traits: Map<TraitType<*>, Trait> = emptyMap()


    init {
        /* Ensure that all IndexTx are on the same entity. */
        require(indexes.asSequence().map { it.first.dbo.parent }.distinct().count() == 1) {
            "All IndexTx for an INDEX INTERSECTION SCAN must be on the same entity. This is a programmer's error!"
        }

        /* Obtain entity and produce statistics. */
        val entityTx = this.indexes.first().first.parent
        val entityProduces = entityTx.listColumns()

        /* Produce statistics. */
        for (binding in this.columns) {
            val column = entityProduces.find { it == binding.physical } ?: throw IllegalArgumentException("Column $binding does not exist in entity ${entityTx.dbo.name}.")
            this.statistics[column] = entityTx.statistics(column)
        }
    }

    /**
     * Creates and returns a copy of this [IndexScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [IndexScanPhysicalOperatorNode].
     */
    override fun copy() = IndexIntersectionScanPhysicalOperatorNode(this.groupId, this.columns.map { it.copy() }, this.indexes)

    /**
     * An [IndexScanPhysicalOperatorNode] is always executable
     *
     * @param ctx The [QueryContext] to check.
     * @return True
     */
    override fun canBeExecuted(ctx: QueryContext): Boolean = true

    /**
     * Converts this [IndexScanPhysicalOperatorNode] to a [IndexScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        /** Generate and return IndexScanOperator. */
        return IndexIntersectionScanOperator(this.groupId, this.indexes, ctx)
    }

    /** Generates and returns a [String] representation of this [EntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.indexes.joinToString(",") { "${it.first.dbo.type}, ${it.second}" }}]"

    /**
     * Generates and returns a [Digest] for this [IndexScanPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.columns.hashCode().toLong()
        for (index in this.indexes) {
            result = 31 * result + index.first.hashCode()
            result = 31 * result + index.second.hashCode()
        }
        return result
    }
}