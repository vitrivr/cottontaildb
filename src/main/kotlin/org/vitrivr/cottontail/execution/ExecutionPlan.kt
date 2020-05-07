package org.vitrivr.cottontail.execution

import com.github.dexecutor.core.DefaultDexecutor
import com.github.dexecutor.core.DexecutorConfig
import com.github.dexecutor.core.ExecutionConfig
import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskProvider
import org.vitrivr.cottontail.execution.tasks.ExecutionPlanException
import org.vitrivr.cottontail.execution.tasks.ExecutionPlanSetupException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.recordset.Recordset
import java.util.*
import java.util.concurrent.ExecutorService
import kotlin.collections.HashMap


/**
 * A configurable execution plan for Cottontail DB queries. This class can be used to specify exactly,
 * how a query should be executed. The execution plan can be configured, by adding different,
 * interdependent [ExecutionTask]s to the [ExecutionPlan]
 *
 * @see ExecutionTask
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class ExecutionPlan(executor: ExecutorService) {

    /** The internal ID of this [ExecutionPlan]. */
    val id: UUID = UUID.randomUUID()

    /** The timestamp at which this [ExecutionPlan] was executed. */
    val timestamp = System.currentTimeMillis()

    /** The internal instance of [ExecutionPlanTaskProvider] that provides this [ExecutionPlan] with [ExecutionTask] instances. */
    private val provider: ExecutionPlanTaskProvider = this.ExecutionPlanTaskProvider()

    /** The [DexecutorConfig] used to setup the DAG execution. */
    private val config = DexecutorConfig(executor, this.provider)

    /**
     * Traverses the given [ExecutionStage] to its root and adds all relevant [ExecutionStage]s to this [ExecutionPlan]
     *
     * @param stage The [ExecutionStage] to compile into an [ExecutionPlan].
     */
    fun compileStage(stage: ExecutionStage) {
        val root = stage.root
        var current: ExecutionStage? = root
        do {
            this.addStage(current!!)
            current = current.child
        } while (current != null)
    }

    /**
     * Adds a new [ExecutionStage] to this [ExecutionPlan].
     *
     * @param stage The [ExecutionStage] to add to this [ExecutionPlan].
     */
    private fun addStage(stage: ExecutionStage) {
        when {
            stage.parent == null -> {
                /** Case 1: Independent stage. */
                stage.tasks.forEach {
                    this.provider.addTask(it)
                    this.config.dexecutorState.addIndependent(it.id)
                }
            }
            stage.mergeType == ExecutionStage.MergeType.ALL -> {
                stage.tasks.forEach { inner ->
                    this.provider.addTask(inner)
                    stage.parent!!.tasks.forEach { outer ->
                        this.config.dexecutorState.addDependency(outer.id, inner.id)
                    }
                }
            }
            stage.mergeType == ExecutionStage.MergeType.ONE -> {
                if (stage.tasks.size != stage.parent?.tasks?.size) throw ExecutionPlanException(this@ExecutionPlan, "The number of tasks must correspond for both ExecutionStages for merge type ${ExecutionStage.MergeType.ONE} but don't (t_i = ${stage.tasks.size}, t_o = ${stage.parent?.tasks?.size}).")
                stage.tasks.forEachIndexed { index, inner ->
                    this.provider.addTask(inner)
                    this.config.dexecutorState.addDependency(stage.parent!!.tasks[index].id, inner.id)
                }
            }
        }
    }

    /**
     * Executes this [ExecutionPlan] and returns the results.
     *
     * @return The resulting [Recordset]
     * @throws ExecutionPlanException If any error occurred during execution.
     */
    fun execute(): Recordset {
        /* Prepare a final stage (it will hold the results of the execution plan). */
        /* Execute plan and check for errors. */
        val results = DefaultDexecutor(this.config).execute(ExecutionConfig.TERMINATING)
        if (results.errored.size > 0) {
            throw ExecutionPlanException(this, results.errored.first().message)
        }

        /* If no error occurred, then check + return results of final stage. */
        if (results.success.size == 0) {
            throw ExecutionPlanException(this, "Graph did not produce any results.")
        }
        return results.success.last().result
    }

    /**
     * The [TaskProvider] instance used by [ExecutionPlan]. It is backed by a HashMap.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    private inner class ExecutionPlanTaskProvider : TaskProvider<String, Recordset> {

        /** List of [Task]s that will be executed in this [ExecutionPlan]. */
        private val tasks: HashMap<String, ExecutionTask> = HashMap()

        /**
         * Returns the task for the given ID.
         *
         * @param id The ID of the requested task.
         * @return The [Task] for
         */
        override fun provideTask(id: String?): ExecutionTask = tasks[id]
                ?: throw ExecutionPlanException(this@ExecutionPlan, "The task provider failed to provide a task for ID '$id'.")

        /**
         * Adds a [Task] task to this [ExecutionPlan]. By doing so, the [Task] will
         * receive a unique ID, which will be returned by the method.
         *
         * @param task The [Task] that should be added.
         * @return The ID of the task in the [ExecutionPlan]
         */
        fun addTask(task: ExecutionTask) {
            if (!this.tasks.containsKey(task.id)) {
                this.tasks[task.id] = task
            } else {
                throw ExecutionPlanSetupException(this@ExecutionPlan, "A task with ID ${task.id} already exists for execution plan.")
            }
        }
    }
}

