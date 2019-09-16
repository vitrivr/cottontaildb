package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue
import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a sequential boolean kNN on a float [Column] of the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
internal class LinearEntityScanKnnTask<T: Any>(val entity: Entity, val knn: KnnPredicate<T>, val predicate: BooleanPredicate? = null) : ExecutionTask("LinearEntityScanKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values. */
    private val knnSet = knn.query.map { HeapSelect<ComparablePair<Long,Double>>(this.knn.k) }

    /** List of the [ColumnDef] this instance of [LinearEntityScanKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef("${entity.fqn}.distance", ColumnType.forName("DOUBLE")))

    /** The cost of this [LinearEntityScanKnnTask] is constant */
    override val cost = this.entity.statistics.columns * (this.knn.operations * 1e-5 + (this.predicate?.operations ?: 0) * 1e-5).toFloat()

    /**
     * Executes this [LinearEntityScanKnnTask]
     */
    override fun execute(): Recordset {
        /* Extract the necessary data. */
        val columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(predicate?.columns?.toTypedArray() ?: emptyArray())

        /* Execute kNN lookup. */
        this.entity.Tx(readonly = true, columns = columns).begin { tx ->
            tx.forEach {
                if (this.predicate == null || this.predicate.matches(it)) {
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
            }
            true
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(this.produces)
        for (knn in this.knnSet) {
            for (i in 0 until knn.size) {
                dataset.addRowUnsafe(knn[i].first, arrayOf(DoubleValue(knn[i].second)))
            }
        }
        return dataset
    }
}