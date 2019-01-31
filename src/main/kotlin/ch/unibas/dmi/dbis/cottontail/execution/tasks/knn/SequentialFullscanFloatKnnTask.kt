package ch.unibas.dmi.dbis.cottontail.execution.tasks.knn

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.column.FloatArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.TaskExecutionException

import ch.unibas.dmi.dbis.cottontail.execution.tasks.TaskSetupException
import ch.unibas.dmi.dbis.cottontail.execution.tasks.knn.KnnTask.DISTANCE_COL

import ch.unibas.dmi.dbis.cottontail.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.knn.metrics.DistanceFunction
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset

import com.github.dexecutor.core.task.Task

import java.util.*


/**
 * A [Task] that executes a sequential scan kNN on a float [Column] of the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class SequentialFullscanFloatKnnTask(
        private val entity: Entity,
        private val column: ColumnDef<*>,
        private val query: FloatArray,
        private val distance: DistanceFunction = Distance.L2,
        private val k: Int = 500
): ExecutionTask("KnnFullscan[${entity.fqn}][${column.name}][${distance::class.simpleName}][$k][q=${query.hashCode()}]") {


    /** Assert that the provided [ColumnDef] complies with the parameters specified in [FullscanFloatKnnTask]. */
    init {
        if (column.size != query.size) {
            throw TaskSetupException(this, "The size of the specified column ${column.name} (dc=${column.size}) does not match the size of the query vector (dq=${query.size})!")
        }

        if (column.type !is FloatArrayColumnType) {
            throw TaskSetupException(this, "The specified column ${column.name} does not have the correct type!")
        }
    }

    /**
     * Executes a full-scan kNN lookup using the provided parameters (query vector, distance metric and k).
     *
     * @return The resulting [Recordset] containing the tupleId and the calculated distance.
     */
    override fun execute(): Recordset {
        /* Make some checks. */
        if (this.column.size != query.size) {
            throw TaskExecutionException(this, "Size of the column '${entity.name}.${column.name}' (dc=${column.size}) and query vector (dq=${query.size}) don't match.")
        }

        /* Execute kNN lookup. */
        val knn = HeapSelect<ComparablePair<Long,Double>>(this.k)
        this.entity.Tx(true).begin { tx ->
            tx.forEachColumn({ tid, v: FloatArray ->
                val dist = this.distance(this.query, v)
                knn.add(ComparablePair(tid, dist))
            }, this.column)
            true
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(DISTANCE_COL)
        for (i in 0 until knn.size) {
            dataset.addRow(knn[i].first, knn[i].second)
        }
        return dataset
    }
}