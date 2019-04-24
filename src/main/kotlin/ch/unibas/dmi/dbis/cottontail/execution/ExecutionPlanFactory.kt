package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.*

import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionStage
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityIndexedFilterTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.*
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntityCountProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.EntityExistsProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityLinearScanFilterTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetCountProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetExistsProjectionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetSelectProjectionTask


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
                ProjectionType.SELECT -> plan.addTask(ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityLinearScanTask(entity, projectionClause.columns))
                ProjectionType.COUNT -> plan.addTask(EntityCountProjectionTask(entity))
                ProjectionType.EXISTS -> plan.addTask(EntityExistsProjectionTask(entity))
            }
        } else if (knnClause != null) {
            val stage1 = KnnTask.entityScanTaskForPredicate(entity, knnClause, whereClause)
            val stage2 = when (projectionClause.type) {
                ProjectionType.SELECT -> RecordsetSelectProjectionTask(entity, projectionClause.columns)
                ProjectionType.COUNT -> RecordsetCountProjectionTask()
                ProjectionType.EXISTS -> RecordsetExistsProjectionTask()
            }

            /* Add tasks to ExecutionPlan. */
            plan.addTask(stage1)
            plan.addTask(stage2, stage1.id)
        } else if (whereClause != null) {
            val stage1 = EntityLinearScanFilterTask(entity, whereClause)
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

    /**
     *
     */
    fun planAndLayoutWhere(entity: Entity, whereClause: BooleanPredicate, plan: ExecutionPlan) {

        /* Generate empty list of execution branches. */
        val candidates = mutableListOf<ExecutionTask>()

        /* Add default case 1: Full table scan. */
        if (entity.canProcess(whereClause)) {
            candidates.add(EntityLinearScanFilterTask(entity, whereClause))
        }

        /* Add default case 2: Cheapest index for full query. */
        val indexes = entity.allIndexes()
        val index = indexes.filter { it.canProcess(whereClause) }.sortedBy { it.cost(whereClause) }.firstOrNull()
        if (index != null) {
            candidates.add(EntityIndexedFilterTask(entity, whereClause, index))
        }

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
    }
}


