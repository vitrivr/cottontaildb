package org.vitrivr.cottontail.execution.tasks.entity.knn

import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.math.knn.selection.Selection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [ExecutionTask] that executes a sequential kNN combined with a [BooleanPredicate] on a
 * [Column][org.vitrivr.cottontail.database.column.Column] of the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.3
 */
class BooleanIndexedKnnTask<T: VectorValue<*>>(val entity: Entity, val knn: KnnPredicate<T>, val predicate: BooleanPredicate, indexHint: Index) : ExecutionTask("BooleanIndexedKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][$predicate][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values. */
    private val knnSet: List<Selection<ComparablePair<Long, DoubleValue>>> = if (this.knn.k == 1) {
        this.knn.query.map { MinSingleSelection<ComparablePair<Long, DoubleValue>>() }
    } else {
        this.knn.query.map { MinHeapSelection<ComparablePair<Long, DoubleValue>>(this.knn.k) }
    }

    /** List of the [ColumnDef] this instance of [BooleanIndexedKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    /** The type of the [Index] that should be used.*/
    private val type = indexHint.type

    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = emptyArray()).query {tx ->
        val index = tx.indexes(this.predicate.columns.toTypedArray(), this.type).first()
        val action: (Record) -> Unit = if (this.knn.weights != null) {
            {
                val value = it[this.knn.column]
                if (value != null) {
                    this.knn.query.forEachIndexed { i, query ->
                        this.knnSet[i].offer(ComparablePair(it.tupleId, this.knn.distance(query, value, this.knn.weights[i])))
                    }
                }
            }
        } else {
            {
                val value = it[this.knn.column]
                if (value != null) {
                    this.knn.query.forEachIndexed { i, query ->
                        this.knnSet[i].offer(ComparablePair(it.tupleId, this.knn.distance(query, value)))
                    }
                }
            }
        }

        index.forEach(this.predicate, action)

        /* Generate dataset and return it. */
        return@query KnnUtilities.selectToRecordset(this.produces.first(), this.knnSet)
    } ?: Recordset(this.produces, capacity = 0)

}