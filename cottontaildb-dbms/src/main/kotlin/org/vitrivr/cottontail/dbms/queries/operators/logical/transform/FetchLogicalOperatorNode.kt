package org.vitrivr.cottontail.dbms.queries.operators.logical.transform

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents fetching certain [ColumnDef] from a specific
 * [Entity] and adding them to the list of requested columns.
 *
 * This can be used for deferred fetching of columns, which can lead to optimized performance for queries
 * that involve pruning the result set (e.g. filters or nearest neighbour search).
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class FetchLogicalOperatorNode(input: Logical, val entity: EntityTx, val fetch: List<Binding.Column>) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Fetch"
    }

    init {
        require(this.fetch.all { it.physical != null }) { "FetchLogicalOperatorNode can only fetch physical columns."  }
    }

    /** The name of this [FetchLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FetchLogicalOperatorNode] accesses the [ColumnDef] of its input + the columns to be fetched. */
    override val columns: List<Binding.Column> by lazy {
        super.columns + this.fetch
    }

    /**
     * Creates a copy of this [FetchLogicalOperatorNode].
     *
     * @param input The new input [Logical]
     * @return Copy of this [FetchLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): FetchLogicalOperatorNode {
        require(input.size == 1) { "The input arity for FetchLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return FetchLogicalOperatorNode(input = input[0], entity = this.entity, fetch = this.fetch)
    }

    /**
     * Returns a [FetchPhysicalOperatorNode] representation of this [FetchLogicalOperatorNode]
     *
     * @return [FetchPhysicalOperatorNode]
     */
    override fun implement(): Physical = FetchPhysicalOperatorNode(this.input.implement(), this.entity, this.fetch)

    /** Generates and returns a [String] representation of this [FetchLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}(${this.fetch.joinToString(",") { it.physical!!.name.toString() }})"

    /**
     * Generates and returns a [Digest] for this [FetchLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.dbo.name.hashCode().toLong()
        result += 33L * result + this.fetch.hashCode()
        return result
    }
}