package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * A [LogicalNodeExpression] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class EntityScanLogicalNodeExpression(val entity: Entity) : NodeExpression.LogicalNodeExpression() {

    /** For the sake of internal reasoning, we assume that all columns are being loaded. */
    protected val columns = this.entity.allColumns()

    /** [EntityScanLogicalNodeExpression] are always root node expressions and thus have an arity of 0. */
    override val inputArity: Int = 0

    /**
     * Simple (full-)table scan.
     */
    class EntityFullScanLogicalNodeExpression(entity: Entity) : EntityScanLogicalNodeExpression(entity) {
        /**
         * Returns a copy of this [EntityScanLogicalNodeExpression]
         *
         * @return Copy of this [EntityScanLogicalNodeExpression]
         */
        override fun copy(): EntityFullScanLogicalNodeExpression = EntityFullScanLogicalNodeExpression(this.entity)
    }

    /**
     * Sampled table scan, i.e., a scan that returns a random subset of the [Entity].
     */
    class EntitySampleLogicalNodeExpression(entity: Entity, val size: Long, val seed: Long = System.currentTimeMillis()) : EntityScanLogicalNodeExpression(entity) {
        /** [EntityScanLogicalNodeExpression] are always root node expressions and thus have an arity of 0. */
        override val inputArity: Int = 0

        /**
         * Returns a copy of this [EntitySampleLogicalNodeExpression]
         *
         * @return Copy of this [EntitySampleLogicalNodeExpression]
         */
        override fun copy(): EntitySampleLogicalNodeExpression = EntitySampleLogicalNodeExpression(this.entity, this.size, this.seed)
    }
}