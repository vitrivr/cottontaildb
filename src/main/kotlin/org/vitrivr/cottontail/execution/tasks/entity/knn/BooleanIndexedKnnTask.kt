package org.vitrivr.cottontail.execution.tasks.entity.knn

import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.BooleanPredicate
import org.vitrivr.cottontail.database.queries.KnnPredicate
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.math.knn.ComparablePair
import org.vitrivr.cottontail.math.knn.HeapSelect
import org.vitrivr.cottontail.math.knn.metrics.DistanceFunction
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.VectorValue

class BooleanIndexedKnnTask<T : Any>(val entity: Entity, val knn: KnnPredicate<T>, val predicate: BooleanPredicate, indexHint: Index) : ExecutionTask("BooleanIndexedKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][$predicate][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values. */
    private val knnSet = knn.query.map { HeapSelect<ComparablePair<Long, Double>>(this.knn.k) }

    /** List of the [ColumnDef] this instance of [BooleanIndexedKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    override val cost = (indexHint.cost(this.predicate) * (this.knn.operations + this.predicate.operations) * 1e-5).toFloat()

    /** The type of the [Index] that should be used.*/
    private val type = indexHint.type

    private inline fun weightedDist(a: VectorValue<T>, b: VectorValue<T>, distanceFunction: DistanceFunction<T>, weights: VectorValue<FloatArray>?): Double {
        return distanceFunction(a, b, weights!!)
    }

    private inline fun nonWeightedDist(a: VectorValue<T>, b: VectorValue<T>, distanceFunction: DistanceFunction<T>, weights: VectorValue<FloatArray>?): Double {
        return distanceFunction(a, b)
    }

    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = emptyArray()).query { tx ->
        val index = tx.indexes(this.predicate.columns.toTypedArray(), this.type).first()

        val distance = if (this.knn.weights != null) {
            ::weightedDist
        } else {
            ::nonWeightedDist
        }

        index.forEach(this.predicate) {

            val value = it[this.knn.column]
            if (value is VectorValue<T>) {
                this.knn.query.forEachIndexed { i, query ->
                    this.knnSet[i].add(ComparablePair(it.tupleId, distance(query, value, this.knn.distance, this.knn.weights?.get(i))))
                }
            }

        }

        /* Generate dataset and return it. */
        val dataset = Recordset(this.produces, capacity = (this.knnSet.size * this.knn.k).toLong())
        for (knn in this.knnSet) {
            for (i in 0 until knn.size) {
                dataset.addRowUnsafe(knn[i].first, arrayOf(DoubleValue(knn[i].second)))
            }
        }

        return@query dataset

    } ?: Recordset(this.predicate.columns.toTypedArray(), capacity = 0)

}