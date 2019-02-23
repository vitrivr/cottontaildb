package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset
import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a sequential scan kNN on a long [Column] of the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class LinearEntityScanIntKnnTask(val entity: Entity, val knn: KnnPredicate<IntArray>, val predicate: BooleanPredicate? = null) : ExecutionTask("KnnFullscan[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {
    override fun execute(): Recordset {
        /* Extract the necessary data. */
        val query = this.knn.queryAsIntArray()
        val weights = this.knn.weightsAsFloatArray()
        val columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(predicate?.columns?.toTypedArray() ?: emptyArray())

        /* Execute kNN lookup. */
        val knn = HeapSelect<ComparablePair<Long,Double>>(this.knn.k)
        this.entity.Tx(true).begin { tx ->
            tx.forEach({
                if (this.predicate == null || this.predicate.matches(it)) {
                    val value = it[this.knn.column]
                    if (value != null) {
                        if (weights != null) {
                            val dist = this.knn.distance(query, value, weights)
                            knn.add(ComparablePair(it.tupleId!!, dist))
                        } else {
                            val dist = this.knn.distance(query, value)
                            knn.add(ComparablePair(it.tupleId!!, dist))
                        }

                    }
                }
            }, columns)
            true
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(arrayOf(KnnTask.DISTANCE_COL))
        for (i in 0 until knn.size) {
            dataset.addRow(knn[i].first, knn[i].second)
        }
        return dataset
    }
}