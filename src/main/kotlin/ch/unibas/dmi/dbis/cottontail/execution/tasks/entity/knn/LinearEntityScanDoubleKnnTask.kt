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
import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a sequential boolean kNN on a float [Column] of the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class LinearEntityScanDoubleKnnTask(val entity: Entity, val knn: KnnPredicate<DoubleArray>, val predicate: BooleanPredicate? = null) : ExecutionTask("LinearEntityScanDoubleKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {

    /** The cost of this [LinearEntityScanDoubleKnnTask] is constant */
    override val cost = this.entity.statistics.columns * (this.knn.operations * 1e-5 + (this.predicate?.operations ?: 0) * 1e-5).toFloat()

    /** List of the [ColumnDef] this instance of [LinearEntityScanDoubleKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef("${entity.fqn}.distance", ColumnType.forName("DOUBLE")))

    /**
     * Executes this [LinearEntityScanDoubleKnnTask]
     */
    override fun execute(): Recordset {
        /* Extract the necessary data. */
        val query = this.knn.queryAsDoubleArray()
        val weights = this.knn.weightsAsDoubleArray()
        val columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(predicate?.columns?.toTypedArray() ?: emptyArray())

        /* Execute kNN lookup. */
        val knn = HeapSelect<ComparablePair<Long,Double>>(this.knn.k)
        this.entity.Tx(readonly = true, columns = columns).begin { tx ->
            tx.forEach {
                if (this.predicate == null || this.predicate.matches(it)) {
                    val value = it[this.knn.column]
                    if (value != null) {
                        if (weights != null) {
                            val dist = this.knn.distance(query, value.value, weights)
                            knn.add(ComparablePair(it.tupleId, dist))
                        } else {
                            val dist = this.knn.distance(query, value.value)
                            knn.add(ComparablePair(it.tupleId, dist))
                        }

                    }
                }
            }
            true
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(this.produces)
        for (i in 0 until knn.size) {
            dataset.addRowUnsafe(knn[i].first, arrayOf(DoubleValue(knn[i].second)))
        }
        return dataset
    }
}