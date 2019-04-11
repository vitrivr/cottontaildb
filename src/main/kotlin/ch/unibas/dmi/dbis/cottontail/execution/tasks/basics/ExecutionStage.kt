package ch.unibas.dmi.dbis.cottontail.execution.tasks.basics


/**
 * An class for organizing linearly dependent [ExecutionTask]s into a single [ExecutionStage]. This class can be used
 * to organize tasks. It does not influence the execution of the individual [ExecutionTask]s.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
internal class ExecutionStage {

    /** List of [ExecutionTask]s contained in this [ExecutionStage]. */
    val tasks = mutableListOf<ExecutionTask>()

    /** Total cost incurred by executing this [ExecutionStage]. */

    val cost
        get() = tasks.map { it.cost }.sum()

    /**
     * Adds the provided [ExecutionTask] to this [ExecutionStage].
     *
     * @param The [ExecutionTask] to add.
     * @return Reference to this [ExecutionStage].
     */
    fun addTask(task: ExecutionTask): ExecutionStage {
        if (!this.tasks.contains(task)) {
            this.tasks.add(task)
        }
        return this
    }
}