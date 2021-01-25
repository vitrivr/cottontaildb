package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.model.basics.Record

/**
 * A [InsertPhysicalNodeExpression] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class InsertPhysicalNodeExpression(val entity: Entity, val record: Record) :
    UnaryPhysicalNodeExpression() {
    override val outputSize: Long = 1L

    override val cost: Cost
        get() = Cost(io = record.columns.size * Cost.COST_DISK_ACCESS_WRITE)

    override fun copy(): InsertPhysicalNodeExpression =
        InsertPhysicalNodeExpression(this.entity, this.record)

    override fun toOperator(engine: TransactionManager): Operator =
        InsertOperator(this.entity, this.record)
}