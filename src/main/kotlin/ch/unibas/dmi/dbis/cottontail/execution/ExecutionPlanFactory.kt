package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.database.column.DoubleArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.FloatArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.IntArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.LongArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Projection
import ch.unibas.dmi.dbis.cottontail.database.queries.ProjectionType
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.LinearEntityScanDoubleKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.LinearEntityScanFloatKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.LinearEntityScanIntKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.LinearEntityScanLongKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntityCountProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntityExistsProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.scan.LinearEntityFilterScanTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetCountProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetExistsProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetSelectProjectionTask
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException


/**
 *
 */
internal class ExecutionPlanFactory (val executionEngine: ExecutionEngine) {
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
    fun simpleExecutionPlan(entity: Entity, projectionClause: Projection, whereClause: BooleanPredicate? = null, knnClause: KnnPredicate<*>? = null): ExecutionPlan {
        val plan = this.executionEngine.newExecutionPlan()
        if (whereClause == null && knnClause == null) {
            when (projectionClause.type) {
                ProjectionType.SELECT -> plan.addTask(ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.scan.LinearEntityScanTask(entity, projectionClause.columns))
                ProjectionType.COUNT -> plan.addTask(EntityCountProjectionTask(entity))
                ProjectionType.EXISTS -> plan.addTask(EntityExistsProjectionTask(entity))
            }
        } else if (knnClause != null) {
            val stage1 = when(knnClause.column.type) {
                is DoubleArrayColumnType -> LinearEntityScanDoubleKnnTask(entity, knnClause as KnnPredicate<DoubleArray>)
                is FloatArrayColumnType -> LinearEntityScanFloatKnnTask(entity, knnClause as KnnPredicate<FloatArray>)
                is LongArrayColumnType -> LinearEntityScanLongKnnTask(entity, knnClause as KnnPredicate<LongArray>)
                is IntArrayColumnType -> LinearEntityScanIntKnnTask(entity, knnClause as KnnPredicate<IntArray>)
                else -> throw QueryException.QueryBindException("A column of type '${knnClause.column.type} is not supported for kNN queries.")
            }
            val stage2 = when (projectionClause.type) {
                ProjectionType.SELECT -> RecordsetSelectProjectionTask(entity, projectionClause.columns)
                ProjectionType.COUNT -> RecordsetCountProjectionTask()
                ProjectionType.EXISTS -> RecordsetExistsProjectionTask()
            }

            /* Add tasks to ExecutionPlan. */
            plan.addTask(stage1)
            plan.addTask(stage2, stage1.id)
        } else if (whereClause != null) {
            val stage1 = LinearEntityFilterScanTask(entity, whereClause)
            val stage2 = when (projectionClause.type) {
                ProjectionType.SELECT -> RecordsetSelectProjectionTask(entity, projectionClause.columns)
                ProjectionType.COUNT -> RecordsetCountProjectionTask()
                ProjectionType.EXISTS -> RecordsetExistsProjectionTask()
            }

            /* Add tasks to ExecutionPlan. */
            plan.addTask(stage1)
            plan.addTask(stage2, stage1.id)
        }

        return plan
    }
}