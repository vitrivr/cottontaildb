package org.vitrivr.cottontail.execution

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.BooleanPredicate
import org.vitrivr.cottontail.database.queries.KnnPredicate
import org.vitrivr.cottontail.database.queries.Projection
import org.vitrivr.cottontail.database.queries.ProjectionType
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.boolean.EntityIndexedFilterTask
import org.vitrivr.cottontail.execution.tasks.entity.boolean.EntityLinearScanFilterTask
import org.vitrivr.cottontail.execution.tasks.entity.boolean.EntityLinearScanTask
import org.vitrivr.cottontail.execution.tasks.entity.fetch.EntityFetchColumnsTask
import org.vitrivr.cottontail.execution.tasks.entity.knn.EntityScanKnnTask
import org.vitrivr.cottontail.execution.tasks.entity.knn.KnnUtilities
import org.vitrivr.cottontail.execution.tasks.entity.projection.*
import org.vitrivr.cottontail.execution.tasks.recordset.merge.RecordsetMergeKnn
import org.vitrivr.cottontail.execution.tasks.recordset.projection.*
import org.vitrivr.cottontail.execution.tasks.recordset.transform.RecordsetLimitTask
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 *
 */
class ExecutionPlanFactory(val executionEngine: ExecutionEngine) {
    /**
     * Returns an [ExecutionPlan] for the specified, simple query that does not contain any JOINS.
     *
     * @param entity The [Entity] from which to fetch the data.
     * @param projectionClause The [Projection] clause of the query.
     * @param whereClause The [BooleanPredicate] (WHERE) clause of the query.
     * @param knnClause The [KnnPredicate] (KNN) clause of the query.
     *
     * @return The resulting [ExecutionPlan]
     */
    fun simpleExecutionPlan(entity: Entity, projectionClause: Projection, whereClause: BooleanPredicate? = null, knnClause: KnnPredicate<*>? = null, limit: Long = -1, skip: Long = -1): ExecutionPlan {

        val plan = this.executionEngine.newExecutionPlan()

        /* Simple case; scan one entity and done! */
        if (whereClause == null && knnClause == null) {
            val result = when (projectionClause.type) {
                ProjectionType.SELECT -> {
                    val s1 = ExecutionStage(mergeType = ExecutionStage.MergeType.ONE).addTask(EntityLinearScanTask(entity, projectionClause.columns))
                    ExecutionStage(mergeType = ExecutionStage.MergeType.ONE, parent = s1).addTask(RecordsetSelectProjectionTask(projectionClause))
                }
                ProjectionType.COUNT -> ExecutionStage(mergeType = ExecutionStage.MergeType.ONE).addTask(EntityCountProjectionTask(entity))
                ProjectionType.EXISTS -> ExecutionStage(mergeType = ExecutionStage.MergeType.ONE).addTask(EntityExistsProjectionTask(entity))
                ProjectionType.SUM -> ExecutionStage(mergeType = ExecutionStage.MergeType.ONE).addTask(EntitySumProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MAX -> ExecutionStage(mergeType = ExecutionStage.MergeType.ONE).addTask(EntityMaxProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MIN -> ExecutionStage(mergeType = ExecutionStage.MergeType.ONE).addTask(EntityMinProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MEAN -> ExecutionStage(mergeType = ExecutionStage.MergeType.ONE).addTask(EntityMeanProjectionTask(entity, projectionClause.columns.first()))
            }
            plan.compileStage(stage = result)
            return plan
        }

        /* Add select with predicate push-down. */
        var last = if (knnClause != null && whereClause == null) {
            planAndLayoutSimpleKnn(entity, knnClause)
        } else if (whereClause != null && knnClause == null) {
            planAndLayoutWhere(entity, whereClause)
        } else {
            planAndLayoutCombinedKnn(entity, knnClause!!, whereClause!!)
        }

        /* Add SELECT clause (column fetching + projection) */
        last = when (projectionClause.type) {
            ProjectionType.SELECT -> {
                val internal = ExecutionStage(ExecutionStage.MergeType.ONE, last).addTask(EntityFetchColumnsTask(entity, projectionClause.columns))
                ExecutionStage(ExecutionStage.MergeType.ONE, internal).addTask(RecordsetSelectProjectionTask(projectionClause))
            }
            ProjectionType.SUM -> {
                val internal = ExecutionStage(ExecutionStage.MergeType.ONE, last).addTask(EntityFetchColumnsTask(entity, projectionClause.columns))
                ExecutionStage(ExecutionStage.MergeType.ONE, internal).addTask(RecordsetSumProjectionTask(projectionClause))
            }
            ProjectionType.MAX -> {
                val internal = ExecutionStage(ExecutionStage.MergeType.ONE, last).addTask(EntityFetchColumnsTask(entity, projectionClause.columns))
                ExecutionStage(ExecutionStage.MergeType.ONE, internal).addTask(RecordsetMaxProjectionTask(projectionClause))
            }
            ProjectionType.MIN -> {
                val internal = ExecutionStage(ExecutionStage.MergeType.ONE, last).addTask(EntityFetchColumnsTask(entity, projectionClause.columns))
                ExecutionStage(ExecutionStage.MergeType.ONE, internal).addTask(RecordsetMinProjectionTask(projectionClause))
            }
            ProjectionType.MEAN -> {
                val internal = ExecutionStage(ExecutionStage.MergeType.ONE, last).addTask(EntityFetchColumnsTask(entity, projectionClause.columns))
                ExecutionStage(ExecutionStage.MergeType.ONE, internal).addTask(RecordsetMeanProjectionTask(projectionClause))
            }
            ProjectionType.COUNT -> ExecutionStage(ExecutionStage.MergeType.ONE, last).addTask(RecordsetCountProjectionTask())
            ProjectionType.EXISTS -> ExecutionStage(ExecutionStage.MergeType.ONE, last).addTask(RecordsetExistsProjectionTask())
        }

        /* Add LIMIT clause (if required). */
        if (limit > 0 || skip > 0) {
            last = ExecutionStage(ExecutionStage.MergeType.ONE, last).addTask(RecordsetLimitTask(limit, skip))
        }

        /** Add [ExecutionStage] to [ExecutionPlan]. */
        plan.compileStage(stage = last)
        return plan
    }

    /**
     * Plans different execution paths for the given [KnnPredicate] and [BooleanPredicate] and returns the most efficient one in terms of cost.
     *
     * @param entity [Entity] on which to execute the [KnnPredicate]
     * @param knnClause The [KnnPredicate] to execute.
     * @param whereClause The [BooleanPredicate] to execute.
     * @return [ExecutionStage] that is expected to be most efficient in terms of costs.
     */
    private fun planAndLayoutCombinedKnn(entity: Entity, knnClause: KnnPredicate<*>, whereClause: BooleanPredicate): ExecutionStage {
        /* Add default case 1: Full table scan based Knn. */
        val operations = entity.statistics.rows * (knnClause.cost + whereClause.cost)
        val parallelism = (operations / KnnUtilities.OPERATIONS_PER_TASK).toInt().coerceAtLeast(1).coerceAtMost(this.executionEngine.availableThreads)
        val maxTupleId = entity.statistics.maxTupleId
        val blocksize = maxTupleId / parallelism

        /** Prepare kNN stage. */
        val knnStage = ExecutionStage(mergeType = ExecutionStage.MergeType.ONE)
        for (i in 0 until parallelism) {
            knnStage.addTask(EntityScanKnnTask(entity, knnClause, whereClause, blocksize * i + 1L, (i + 1) * blocksize))
        }

        /** Add a merge stage, if parallelism is > 1. */
        return if (parallelism > 1) {
            ExecutionStage(ExecutionStage.MergeType.ALL, knnStage).addTask(RecordsetMergeKnn(entity, knnClause)) /* Create and return a merge stage. */
        } else {
            knnStage
        }
    }

    /**
     * Plans different execution paths for the given [KnnPredicate] and returns the most efficient one in terms of cost.
     *
     * @param entity [Entity] on which to execute the [KnnPredicate]
     * @param knnClause The [KnnPredicate] to execute.
     * @return [ExecutionStage] that is expected to be most efficient in terms of costs.
     */
    private fun planAndLayoutSimpleKnn(entity: Entity, knnClause: KnnPredicate<*>): ExecutionStage {
        /* Add default case 1: Full table scan based Knn. */
        val operations = entity.statistics.rows * knnClause.cost
        val parallelism = (operations / KnnUtilities.OPERATIONS_PER_TASK).toInt().coerceAtLeast(1).coerceAtMost(this.executionEngine.availableThreads)
        val maxTupleId = entity.statistics.maxTupleId
        val blocksize = maxTupleId / parallelism

        /** Prepare kNN stage. */
        val knnStage = ExecutionStage(mergeType = ExecutionStage.MergeType.ONE)
        for (i in 0 until parallelism) {
            knnStage.addTask(EntityScanKnnTask(entity, knnClause, null, blocksize * i + 1L, (i + 1) * blocksize))
        }

        /** Add a merge stage, if parallelism is > 1. */
        return if (parallelism > 1) {
            ExecutionStage(ExecutionStage.MergeType.ALL, knnStage).addTask(RecordsetMergeKnn(entity, knnClause)) /* Create and return a merge stage. */
        } else {
            knnStage
        }
    }

    /**
     * Plans different execution paths for the [BooleanPredicate] and returns the most efficient one in terms of cost.
     *
     * @param entity [Entity] on which to execute the [BooleanPredicate]
     * @param whereClause The [BooleanPredicate] to execute.
     * @return [ExecutionStage] that is expected to be most efficient in terms of costs.
     */
    private fun planAndLayoutWhere(entity: Entity, whereClause: BooleanPredicate): ExecutionStage {

        /* Generate empty list of execution branches. */
        val candidates = mutableListOf<ExecutionStage>()

        /* Add default case 1: Full table scan. */
        if (entity.canProcess(whereClause)) {
            val stage = ExecutionStage(ExecutionStage.MergeType.ONE)
            stage.addTask(EntityLinearScanFilterTask(entity, whereClause))
            candidates.add(stage)
        }

        /* Add default case 2: Cheapest index for full query. */
        val indexes = entity.allIndexes()
        val index = indexes.filter { it.canProcess(whereClause) }.minBy { it.cost(whereClause) }
        if (index != null) {
            val stage = ExecutionStage(ExecutionStage.MergeType.ONE)
            stage.addTask(EntityIndexedFilterTask(entity, whereClause, index))
            candidates.add(stage)
        }

        /* TODO: Try other paths by re-arranging whereClause */

        /* Check if list of candidates contains elements. */
        if (candidates.size == 0) {
            throw QueryException.QueryBindException("Failed to generate a valid execution plan; no path found to satisfy WHERE-clause.")
        }

        /* Take cheapest execution path and return it. */
        candidates.sortBy { it.cost }
        return candidates.first()
    }
}


