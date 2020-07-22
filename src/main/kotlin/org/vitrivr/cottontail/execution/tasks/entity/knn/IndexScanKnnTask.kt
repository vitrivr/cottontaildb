package org.vitrivr.cottontail.execution.tasks.entity.knn

import com.github.dexecutor.core.task.Task
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [Task] that executes a index based kNN on the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IndexScanKnnTask<T : VectorValue<*>>(val entity: Entity, val knnPredicate: KnnPredicate<T>, indexHint: Index) : ExecutionTask("EntityIndexedKnnTask[${entity.fqn}][${knnPredicate.column.name}][${knnPredicate.distance::class.simpleName}][${knnPredicate.k}][q=${knnPredicate.query.hashCode()}]") {

    /** The type of the [Index] that should be used.*/
    private val type = indexHint.type

    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = arrayOf(knnPredicate.column)).query { tx ->
        val index = tx.indexes(this.knnPredicate.columns.toTypedArray(), this.type).first()
        index.filter(this.knnPredicate)
    } ?: Recordset(this.knnPredicate.columns.toTypedArray(), capacity = 0)
}