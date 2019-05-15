package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.*

import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionStage
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityIndexedFilterTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.*
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityLinearScanFilterTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.fetch.EntityFetchColumnsTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.*
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntityCountProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntityExistsProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntityMaxProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntityMinProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntitySumProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.*
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetCountProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetExistsProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetMaxProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetSelectProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetSumProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.transform.RecordsetLimitTask
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
    fun simpleExecutionPlan(entity: Entity, projectionClause: Projection, whereClause: BooleanPredicate? = null, knnClause: KnnPredicate<*>? = null, limit: Long = -1, skip: Long = -1): ExecutionPlan {
        val plan = this.executionEngine.newExecutionPlan()

        /* Add kNN and/or WHERE clause. */
        var last: String = if (whereClause == null && knnClause == null) {
            when (projectionClause.type) {
                ProjectionType.SELECT -> plan.addTask(ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityLinearScanTask(entity, projectionClause.columns))
                ProjectionType.COUNT ->  plan.addTask(EntityCountProjectionTask(entity))
                ProjectionType.EXISTS -> plan.addTask(EntityExistsProjectionTask(entity))
                ProjectionType.SUM -> plan.addTask(EntitySumProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MAX -> plan.addTask(EntityMaxProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MIN -> plan.addTask(EntityMinProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MEAN -> plan.addTask(EntityMeanProjectionTask(entity, projectionClause.columns.first()))
            }
        } else if (knnClause != null) {
            plan.addTask(KnnTask.entityScanTaskForPredicate(entity, knnClause, whereClause))
        } else {
            plan.addStage(planAndLayoutWhere(entity, whereClause!!))
        }

        /* Add SELECT clause (column fetching + projection) */
        last = when (projectionClause.type) {
            ProjectionType.SELECT -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetSelectProjectionTask(projectionClause), internal)
            }
            ProjectionType.SUM -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetSumProjectionTask(projectionClause), internal)
            }
            ProjectionType.MAX -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetMaxProjectionTask(projectionClause), internal)
            }
            ProjectionType.MIN -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetMinProjectionTask(projectionClause), internal)
            }
            ProjectionType.MEAN -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetMeanProjectionTask(projectionClause), internal)
            }
            ProjectionType.COUNT -> {
                plan.addTask(RecordsetCountProjectionTask(), last)
            }
            ProjectionType.EXISTS -> {
                plan.addTask(RecordsetExistsProjectionTask(), last)
            }
        }

        /* TODO: ORDER BY clause. */

        /* Add LIMIT clause (if required). */
        if (limit > -1 || skip > -1) {
            plan.addTask(RecordsetLimitTask(limit, skip))
        }

        return plan
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
            val stage = ExecutionStage()
            stage.addTask(EntityLinearScanFilterTask(entity, whereClause))
            candidates.add(stage)
        }

        /* Add default case 2: Cheapest index for full query. */
        val indexes = entity.allIndexes()
        val index = indexes.filter { it.canProcess(whereClause) }.sortedBy { it.cost(whereClause) }.firstOrNull()
        if (index != null) {
            val stage = ExecutionStage()
            stage.addTask(EntityIndexedFilterTask(entity, whereClause, index))
            candidates.add(stage)
        }


        /* TODO: Explore more strains of execution by decomposing the WHERE-clause. */
        /* Now start decomposing query and generating alternative strains of execution. */


        /* if (whereClause is CompoundBooleanPredicate) {
            var decomposed = listOf(whereClause.p1, whereClause.p2)
            var operators = listOf(whereClause.connector)
            var depth = 1.0
            outer@ while (decomposed.isNotEmpty()) {

                var newDecomposed = mutableListOf<BooleanPredicate>()
                val stage = ExecutionStage()
                inner@ for (i in 0 until Math.pow(2.0,depth).toInt()) {


                    if (entity.canProcess(decomposed[i])) {
                        stage.addTask(EntityLinearScanFilterTask(entity, whereClause))
                    }
                    decomposed[i]
                    val clause = decomposed[i]
                    if (clause is CompoundBooleanPredicate) {
                        stage.addTask(EntityLinearScanFilterTask(entity, clause.p1))
                        stage.addTask(EntityLinearScanFilterTask(entity, clause.p2))
                    }
                }
                indexes.filter { it.canProcess(it) }

                val valid = decomposed.all { it is CompoundBooleanPredicate }
                if (valid) {

                }
                depth += 1
            }
        } */

        /* Check if list of candidates contains elements. */
        if (candidates.size == 0) {
            throw QueryException.QueryBindException("Failed to generate a valid execution plan; no path found to satisfy WHERE-clause.")
        }

        /* Take cheapest execution path and return it. */
        candidates.sortBy { it.cost }
        return candidates.first()
    }
}


