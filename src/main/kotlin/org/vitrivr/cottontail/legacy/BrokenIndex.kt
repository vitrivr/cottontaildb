package org.vitrivr.cottontail.legacy

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BrokenIndex(
    override val name: Name.IndexName,
    override val parent: Entity,
    override val path: Path,
    override val type: IndexType,
    override val columns: Array<ColumnDef<*>>
    )
: Index {
    override val closed: Boolean = true
    override fun close() { /* No Op. */ }
    override val produces: Array<ColumnDef<*>> = emptyArray()
    override val supportsIncrementalUpdate: Boolean = false
    override val supportsPartitioning: Boolean = false
    override val dirty: Boolean = false
    override fun canProcess(predicate: Predicate): Boolean = false
    override fun cost(predicate: Predicate): Cost = Cost.INVALID
    override fun newTx(context: TransactionContext): IndexTx {
        throw UnsupportedOperationException("Operation not supported on legacy DBO.")
    }
}