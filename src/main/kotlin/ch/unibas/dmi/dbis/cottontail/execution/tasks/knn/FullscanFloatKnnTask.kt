package ch.unibas.dmi.dbis.cottontail.execution.tasks.knn

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.column.FloatArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask

import ch.unibas.dmi.dbis.cottontail.execution.tasks.TaskSetupException

import ch.unibas.dmi.dbis.cottontail.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.knn.metrics.DistanceFunction
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset

import com.github.dexecutor.core.task.Task

import java.util.*


/**
 * A [Task] that executed a fullscan kNN on a specified [Entity]
 */
internal class FullscanFloatKnnTask(
        private val entity: Entity,
        private val column: ColumnDef<*>,
        private val query: FloatArray,
        private val distance: DistanceFunction = Distance.L2,
        private val k: Int = 500
): ExecutionTask("KnnFullscan[${entity.fqn}][${column.name}][${distance::class.simpleName}][$k][q=${query.hashCode()}]") {


    /** Assert that the provided [ColumnDef] complies with the parameters specified in [FullscanFloatKnnTask]. */
    init {
        if (column.size != query.size) {
            throw TaskSetupException(this, "The size of the specified column ${column.name} (dc=${column.size}) does not match the size of the query vector (dq=${query.size}!")
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

        /* Execute kNN lookup. */
        val knn = TreeSet<Pair<Long,Double>> { o1, o2 -> o1.second.compareTo(o2.second) }
        this.entity.Tx(true).begin { tx ->
            tx.forEachColumn({ tid, v: FloatArray ->
                val dist = this.distance(this.query, v)
                if (knn.size <= this.k) {
                    knn.add(Pair(tid, dist))
                } else if (dist < knn.last().second) {
                    knn.pollLast()
                    knn.add(Pair(tid, dist))

                }
            }, this.column)
            true
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(DISTANCE_COL)
        knn.forEach { dataset.addRow(it.first, it.second) }
        return dataset
    }

    /**
     * Companion object for [FullscanFloatKnnTask].
     */
    companion object {
        /** Definition for the distance column. */
        val DISTANCE_COL = ColumnDef.withAttributes("distance", "DOUBLE")
    }
}