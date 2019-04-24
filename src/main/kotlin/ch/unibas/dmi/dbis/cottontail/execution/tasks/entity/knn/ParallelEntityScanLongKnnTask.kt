package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import com.github.dexecutor.core.task.Task
import java.util.concurrent.ConcurrentSkipListSet

/**
 * A [Task] that executes a parallel boolean kNN on a float [Column] of the specified [Entity].
 * Parallelism is achieved through the use of co-routines.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class ParallelEntityScanLongKnnTask(val entity: Entity, val knn: KnnPredicate<LongArray>, val predicate: BooleanPredicate? = null, val parallelism: Short = 2) : ExecutionTask("ParallelEntityScanDoubleKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values. */
    private val knnSet = ConcurrentSkipListSet<ComparablePair<Long,Double>>()

    /** The cost of this [ParallelEntityScanLongKnnTask] is constant */
    override val cost = entity.statistics.columns * (knn.operations + (predicate?.operations ?: 0)).toFloat() / parallelism

    /**
     * Executes this [ParallelEntityScanLongKnnTask]
     */
    override fun execute(): Recordset {
        /* Extract the necessary data. */
        val query = this.knn.queryAsLongArray()
        val weights = this.knn.weightsAsFloatArray()
        val columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(predicate?.columns?.toTypedArray() ?: emptyArray())

        /* Execute kNN lookup. */
        this.entity.Tx(readonly = true, columns = columns).begin { tx ->
            tx.forEach(this.parallelism) {
                if (this.predicate == null || this.predicate.matches(it)) {
                    val value = it[this.knn.column]
                    if (value != null) {
                        if (weights != null) {
                            this.addCandidate(it.tupleId, this.knn.distance(query, value.value, weights))
                        } else {
                            this.addCandidate(it.tupleId, this.knn.distance(query, value.value))
                        }
                    }
                }
            }
            true
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(arrayOf(KnnTask.DISTANCE_COL))
        for (e in knnSet) {
            dataset.addRow(e.first, arrayOf(DoubleValue(e.second)))
        }
        return dataset
    }

    /**
     * Adds a candidate to the list of kNN entries if its distance is smaller than
     * the last entry in the list.
     *
     * @param tupleId The tupleID of the candidate.
     * @param distance: The distance of the candidate.
     */
    @Synchronized
    private fun addCandidate(tupleId: Long, distance: Double) {
        if (this.knnSet.size < this.knn.k) {
            this.knnSet.add(ComparablePair(tupleId, distance))
        } else if (distance <= this.knnSet.last().second) {
            this.knnSet.pollLast()
            this.knnSet.add(ComparablePair(tupleId, distance))
        }
    }
}