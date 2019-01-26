package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset
import com.github.dexecutor.core.DefaultDexecutor
import com.github.dexecutor.core.DexecutorConfig
import com.github.dexecutor.core.ExecutionConfig
import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskProvider
import java.util.*
import java.util.concurrent.ExecutorService


/**
 * A configurable execution plan for Cottontail DB queries. This class can be used to specify exactly,
 * how a query should be executed. The execution plan can be configured, by adding different,
 * interdependent [ExecutionTask]s to the [ExcutionPlan]
 *
 * @see ExecutionTask
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ExecutionPlan(executor: ExecutorService) {

    /** The internal [TaskProvider] instance. Required for the Dexecutor framework. */
    private val provider = object:TaskProvider<Int,Recordset> {

        /** List of [Task]s that will be executed in this [ExecutionPlan]. */
        private val tasks: LinkedList<ExecutionTask> = LinkedList()

        /**
         * Returns the task for the given ID.
         *
         * @param id The ID of the requested task.
         * @return The [Task] for
         */
        override fun provideTask(id: Int?): ExecutionTask = tasks[id!!]

        /**
         * Adds a [Task] task to this [ExecutionPlan]. By doing so, the [Task] will
         * receive a unique ID, which will be returned by the method.
         *
         * @param task The [Task] that should be added.
         * @return The ID of the task in the [ExecutionPlan]
         */
        fun addTask(task: ExecutionTask): Int {
            task.id = this.tasks.size
            this.tasks.add(task)
            return task.id
        }
    }

    /** Instance of [DexecutorConfig] used by this [ExecutionPlan]. */
    private val config = DexecutorConfig(executor, provider)

    /** Instance of [DexecutorConfig] used by this [ExecutionPlan]. */
    private val dexecutor = DefaultDexecutor(config)

    /**
     * Reference to the output [ExecutionTask]. A [ExecutionPlan] can have a single output only.
     */
    var output: ExecutionTask? = null
        private set

    /**
     * Adds a new [ExecutionTask] to this [ExecutionPlan].
     *
     * @param task The [ExecutionTask] to add to the plan.
     * @param dependsOn The ID's of the [ExecutionTask] the new [ExecutionTask] depends on. If none are given, the [ExecutionTask] is independent.
     * @return The ID of the added [ExecutionTask]
     */
    fun addTask(task: ExecutionTask, vararg dependsOn: Int): Int {
        val newId = this.provider.addTask(task)
        if (dependsOn.size > 0) {
            for (id in dependsOn) {
                this.dexecutor.addDependency(id, newId)
            }
        } else {
            this.dexecutor.addIndependent(newId)
        }

        return newId
    }

    /**
     * Adds a new output [ExecutionTask] to this [ExecutionPlan].
     *
     * @param task The [ExecutionTask] that should act as output.
     * @param dependsOn The ID's of the [ExecutionTask] the output [ExecutionTask] depends on.
     * @return The ID of the added [ExecutionTask]
     */
    fun addOutput(task: ExecutionTask): Int {
        val newId = this.provider.addTask(task)
        this.output = task
        this.dexecutor.addAsDependentOnAllLeafNodes(newId)
        return newId
    }

    /**
     *
     */
    fun execute(): Unit {
        if (this.output != null) {
            val output = this.dexecutor.execute(ExecutionConfig.TERMINATING)
            print("Success!")
        }
    }
}