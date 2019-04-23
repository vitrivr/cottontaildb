package ch.unibas.dmi.dbis.cottontail.execution.tasks.basics

import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan


/**
 * An class for organizing [ExecutionTask]s into a single [ExecutionStage].
 *
 * @version 1.0
 * @author Ralph Gasser
 */
internal class ExecutionStage {
    /** */
    val tasks = mutableMapOf<ExecutionTask,Collection<String>>()

    /** Total cost incurred by executing this [ExecutionStage]. */
    val cost: Float = this.tasks.keys.map { it.cost }.sum()

    /**
     * Adds a new [ExecutionTask] to this [ExecutionPlan].
     *
     * @param task The [ExecutionTask] to add to the plan.
     * @param dependsOn The ID's of the [ExecutionTask]s the new [ExecutionTask] depends on. If none are given, the [ExecutionTask] is independent.
     */
    fun addTask(task: ExecutionTask, vararg dependsOn: String) {
        this.tasks[task] = dependsOn.toList()
    }
}