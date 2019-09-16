package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

import kotlin.math.min

internal object KnnTask {
    /** Threshold under which parallelism starts to kick in. TODO: Find optimal value experimentally. */
    private const val KNN_OP_PARALLELISM_THRESHOLD = 819200000L

    /**
     * Constructs a [ExecutionTask] for kNN lookup given the [KnnPredicate] and the optional [BooleanPredicate]. This method
     * optimizes for parallelism given the required operations and the size of the [Entity] involved.
     *
     * @param entity [Entity] on which to perform the kNN lookup.
     * @param knnClause The [KnnPredicate] that specifies the kNN lookup
     * @param whereClause The optional [BooleanPredicate] to apply before doing the kNN lookup
     * @return  The resulting [ExecutionTask]
     *
     * @throws QueryException.QueryBindException If the provided [ColumnDef] does not support kNN lookups.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T: Any> entityScanTaskForPredicate(entity: Entity, knnClause: KnnPredicate<T>, whereClause: BooleanPredicate?) : ExecutionTask {
        val operations = knnClause.query.first().size * entity.statistics.rows * (knnClause.operations + (whereClause?.operations ?: 0))
        val parallelism = min(Math.floorDiv(operations, KNN_OP_PARALLELISM_THRESHOLD).toInt(), Runtime.getRuntime().availableProcessors() / 2).toShort()
        return if (parallelism > 1) {
            ParallelEntityScanKnnTask(entity, knnClause, whereClause, parallelism)
        } else {
            LinearEntityScanKnnTask(entity, knnClause, whereClause)
        }
    }
}