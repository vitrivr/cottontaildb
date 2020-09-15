package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.SourceOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * An abstract [Operator.SourceOperator] that access an [Entity.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractEntityOperator(context: ExecutionEngine.ExecutionContext, private val entity: Entity, override val columns: Array<ColumnDef<*>>) : SourceOperator(context) {
    override val operational: Boolean = true

    /** Transaction used by this [AbstractEntityOperator]. */
    protected var transaction: Entity.Tx? = null

    override fun prepareOpen() {
        this.transaction = this.entity.Tx(readonly = true, columns = this.columns)
    }

    override fun prepareClose() {
        this.transaction!!.close()
        this.transaction = null
    }
}