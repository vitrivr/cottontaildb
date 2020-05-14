package org.vitrivr.cottontail.execution.tasks.entity.knn

import com.github.dexecutor.core.task.Task
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.database.queries.BooleanPredicate
import org.vitrivr.cottontail.database.queries.KnnPredicate
import org.vitrivr.cottontail.execution.tasks.TaskSetupException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.execution.tasks.recordset.merge.RecordsetMergeKnn
import org.vitrivr.cottontail.math.knn.ComparablePair
import org.vitrivr.cottontail.math.knn.HeapSelect
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [Task] that executes a sequential kNN on a [Column][org.vitrivr.cottontail.database.column.Column] of the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.2
 */
class EntityScanKnnTask<T : VectorValue<*>>(val entity: Entity, val knn: KnnPredicate<T>, val predicate: BooleanPredicate? = null, start: Long? = null, end: Long? = null) : ExecutionTask("LinearEntityScanKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {
    /** Set containing the kNN values. */
    private val knnSet = knn.query.map { HeapSelect<ComparablePair<Long, DoubleValue>>(this.knn.k) }

    /** Begin of range that should be scanned by this [EntityScanKnnTask]. */
    private val from = start ?: 1L

    /** End of range that should be scanned by this [EntityScanKnnTask]. */
    private val to = end ?: entity.statistics.maxTupleId

    /** The output [ColumnDef] produced by this [RecordsetMergeKnn]. */
    private val column = ColumnDef(this.entity.fqn.append(KnnUtilities.DISTANCE_COLUMN_NAME), KnnUtilities.DISTANCE_COLUMN_TYPE)

    /** List of the [ColumnDef] this instance of [EntityScanKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(column)

    /** The cost of this [EntityScanKnnTask] is constant */
    override val cost = (this.entity.statistics.columns * this.knn.cost.toFloat() + (predicate?.cost
            ?: 0.0)).toFloat()

    init {
        if (this.from < 1L) throw TaskSetupException(this, "EntityScanKnnTask scan range is invalid (from=${this.from}, to=${this.to}).")
        if (this.from > this.to) throw TaskSetupException(this, "EntityScanKnnTask scan range is invalid (from=${this.from}, to=${this.to}).")
    }

    /**
     * Executes this [EntityScanKnnTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(this.predicate?.columns?.toTypedArray()
            ?: emptyArray())).query { tx ->
        /* Prepare scan action. */
        val action: (Record) -> Unit = if (this.knn.weights != null) {
            {
                val value = it[this.knn.column]
                if (value != null) {
                    this.knn.query.forEachIndexed { i, query ->
                        this.knnSet[i].add(ComparablePair(it.tupleId, this.knn.distance(query, value, this.knn.weights[i])))
                    }
                }
            }
        } else {
            {
                val value = it[this.knn.column]
                if (value != null) {
                    this.knn.query.forEachIndexed { i, query ->
                        this.knnSet[i].add(ComparablePair(it.tupleId, this.knn.distance(query, value)))
                    }
                }
            }
        }

        /* Execute table scan. */
        if (this.predicate != null) {
            tx.forEach(from, to, this.predicate, action)
        } else {
            tx.forEach(from, to, action)
        }

        /** Generate recordset from HeapSelect data structures. */
        KnnUtilities.heapSelectToRecordset(this.column, this.knnSet)

    } ?: Recordset(this.produces, capacity = 0)
}