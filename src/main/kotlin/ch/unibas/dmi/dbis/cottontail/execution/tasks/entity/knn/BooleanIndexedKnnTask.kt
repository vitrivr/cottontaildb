package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue

class BooleanIndexedKnnTask<T: Any>(val entity: Entity, val knn: KnnPredicate<T>, val predicate: BooleanPredicate, indexHint: Index) : ExecutionTask("BooleanIndexedKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][$predicate][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values. */
    private val knnSet = knn.query.map { HeapSelect<ComparablePair<Long,Double>>(this.knn.k) }

    /** List of the [ColumnDef] this instance of [BooleanIndexedKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    override val cost = (indexHint.cost(this.predicate) * (this.knn.operations + this.predicate.operations) * 1e-5).toFloat()

    /** The type of the [Index] that should be used.*/
    private val type = indexHint.type


    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = emptyArray()).query {tx ->
        val index = tx.indexes(this.predicate.columns.toTypedArray(), this.type).first()

        index.forEach(this.predicate) {

            val value = it[this.knn.column]
            if (value is VectorValue<T>) {
                this.knn.query.forEachIndexed { i, query ->
                    if (this.knn.weights != null) {
                        this.knnSet[i].add(ComparablePair(it.tupleId, this.knn.distance(query, value, this.knn.weights[i])))
                    } else {
                        this.knnSet[i].add(ComparablePair(it.tupleId, this.knn.distance(query, value)))
                    }
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