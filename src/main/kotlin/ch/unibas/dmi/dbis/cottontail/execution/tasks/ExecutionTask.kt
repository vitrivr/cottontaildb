package ch.unibas.dmi.dbis.cottontail.execution.tasks

import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset
import com.github.dexecutor.core.task.Task


/**
 * A single task usually executed as part of a query. Such as task generated a [Recordset] by  fetching or
 * transforming data. [ExecutionTask]s are usually part of an [ExecutionPlan].
 *
 * @see ExecutionPlan
 * @see Recordset
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class ExecutionTask: Task<Int, Recordset>() {


    /**
     *
     */
    protected fun first(): Recordset = this.parentResults.first.result

    /**
     *
     */
    protected fun all(): Collection<Recordset> = this.parentResults.all.map { it.result }

    /**
     *
     */
    protected fun allSuccessful(): Collection<Recordset> = this.parentResults.all.filter {it.isSuccess} .map { it.result }

    /**
     * Asserts that the input provided by the parent [ExecutionTask] is unary (i.e. exactly one input
     * that did not fail).
     */
    protected fun assertUnaryInput() {
        if (!this.parentResults.hasAnyResult()) {
            throw TaskExecutionException(this, "Parent task did not provide any output.")
        }

        if (this.parentResults.all.size > 1) {
            throw TaskExecutionException(this, "Parent task did provide more than one output record set. Please foresee a merging stage.")
        }

        if (!this.parentResults.first.isSuccess) {
            throw TaskExecutionException(this, "Output of parent task failed..")
        }
    }
}