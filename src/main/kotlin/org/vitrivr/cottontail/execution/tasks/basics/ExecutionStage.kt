package org.vitrivr.cottontail.execution.tasks.basics

import org.vitrivr.cottontail.execution.ExecutionPlan


/**
 * An class for organizing [ExecutionTask]s into a single [ExecutionStage].
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class ExecutionStage {
    /** List of [ExecutionTask]s that make up this stage. */
    val tasks = mutableMapOf<ExecutionTask, Collection<String>>()

    /** Total cost incurred by executing this [ExecutionStage]. */
    val cost: Float
        get() = this.tasks.keys.map { it.cost }.sum()

    /** The ID of the output task. */
    var output: String? = null

    /**
     * Adds a new [ExecutionTask] to this [ExecutionPlan]. The [ExecutionTask] that was added last will act as output for the [ExecutionStage]
     *
     * @param task The [ExecutionTask] to add to the plan.
     * @param dependsOn The ID's of the [ExecutionTask]s the new [ExecutionTask] depends on. If none are given, the [ExecutionTask] is independent.
     */
    fun addTask(task: ExecutionTask, vararg dependsOn: String) {
        this.tasks[task] = dependsOn.toList()
        output = task.id
    }
}