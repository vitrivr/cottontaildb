package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.config.Global
import ch.unibas.dmi.dbis.cottontail.database.column.DoubleArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.FloatArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.IntArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.LongArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

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
    fun entityScanTaskForPredicate(entity: Entity, knnClause: KnnPredicate<*>, whereClause: BooleanPredicate?) : ExecutionTask {
        val operations = entity.statistics.rows * (knnClause.operations + (whereClause?.operations ?: 0))
        val parallelism = Math.min(Math.floorDiv(operations, KNN_OP_PARALLELISM_THRESHOLD).toInt(), (Math.floorDiv(Global.LOGICAL_THREADS,4))).toShort()
        return when {
            parallelism > 1 && knnClause.column.type is DoubleArrayColumnType -> ParallelEntityScanDoubleKnnTask(entity, knnClause as KnnPredicate<DoubleArray>, whereClause, parallelism)
            parallelism > 1 && knnClause.column.type is FloatArrayColumnType -> ParallelEntityScanFloatKnnTask(entity, knnClause as KnnPredicate<FloatArray>, whereClause, parallelism)
            parallelism > 1 && knnClause.column.type is LongArrayColumnType -> ParallelEntityScanLongKnnTask(entity, knnClause as KnnPredicate<LongArray>, whereClause, parallelism)
            parallelism > 1 && knnClause.column.type is IntArrayColumnType -> ParallelEntityScanIntKnnTask(entity, knnClause as KnnPredicate<IntArray>, whereClause, parallelism)
            parallelism <= 1 && knnClause.column.type is DoubleArrayColumnType -> LinearEntityScanDoubleKnnTask(entity, knnClause as KnnPredicate<DoubleArray>, whereClause)
            parallelism <= 1 && knnClause.column.type is FloatArrayColumnType -> LinearEntityScanFloatKnnTask(entity, knnClause as KnnPredicate<FloatArray>, whereClause)
            parallelism <= 1 && knnClause.column.type is LongArrayColumnType -> LinearEntityScanLongKnnTask(entity, knnClause as KnnPredicate<LongArray>, whereClause)
            parallelism <= 1 && knnClause.column.type is IntArrayColumnType -> LinearEntityScanIntKnnTask(entity, knnClause as KnnPredicate<IntArray>, whereClause)
            else -> throw QueryException.QueryBindException("A column of type '${knnClause.column.type} is not supported for kNN queries.")
        }
    }
}