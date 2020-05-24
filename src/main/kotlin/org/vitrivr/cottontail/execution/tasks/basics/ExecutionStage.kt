package org.vitrivr.cottontail.execution.tasks.basics

import org.vitrivr.cottontail.execution.ExecutionPlan

/**
 * An class for organizing [ExecutionTask]s into a single [ExecutionStage]. Tasks in a single
 * [ExecutionStage] are independent and can be executed in parallel.
 *
 * @version 1.1.1
 * @author Ralph Gasser
 */
class ExecutionStage(val mergeType: MergeType, parent: ExecutionStage? = null) {

    /** The [ExecutionStage] that is executed AFTER this [ExecutionStage]. Can be null */
    var parent: ExecutionStage? = parent
        private set

    /** The [ExecutionStage] that is executed AFTER this [ExecutionStage]. Can be null */
    var child: ExecutionStage? = null
        private set

    /** Gets the root [ExecutionStage] for this [ExecutionStage]. Cannot be null! */
    val root: ExecutionStage
        get() = this.parent?.root ?: this

    /** List of [ExecutionTask]s that make up this stage. */
    val tasks = mutableListOf<ExecutionTask>()

    /** The ID of the output task. */
    val output: List<String>
        get() = this.tasks.map { it.id }

    init {
        this.parent?.child = this
    }

    /**
     * Adds a new [ExecutionTask] to this [ExecutionPlan]. The [ExecutionTask] that was added last will act as output for the [ExecutionStage]
     *
     * @param task The [ExecutionTask] to add to the plan.
     * @param dependsOn The ID's of the [ExecutionTask]s the new [ExecutionTask] depends on. If none are given, the [ExecutionTask] is independent.
     */
    fun addTask(task: ExecutionTask): ExecutionStage {
        this.tasks.add(task)
        return this
    }

    /**
     * Enum that describes how a [ExecutionStage] is merged with the preceding [ExecutionStage].
     */
    enum class MergeType {
        ALL,

        /** [ExecutionStage] waits for all tasks of the preceding [ExecutionStage] to finish. */
        ONE
        /** [ExecutionStage] waits for a single task of the preceding [ExecutionStage] to finish. Requires a 1:1 mapping of [ExecutionTask]s. */
    }
}