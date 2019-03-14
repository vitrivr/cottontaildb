package ch.unibas.dmi.dbis.cottontail.execution.tasks

import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan

/**
 * This exceptions is thrown whenever a single [ExecutionPlan] fails.
 *
 * @param task The [ExecutionPlan] that failed.
 * @param message An error message describing the circumstances.
 */
internal class ExecutionPlanException(plan: ExecutionPlan, message: String) : Throwable("Execution failed for execution plan ${plan.id}: $message.")

/**
 * This exceptions is thrown whenever the setup of an [ExecutionPlan] fails. Errors of this kind are is usually caused
 * by mal-specification.
 *
 * @param task The [ExecutionPlan] that failed.
 * @param message An error message describing the circumstances.
 */
internal class ExecutionPlanSetupException(plan: ExecutionPlan, message: String) : Throwable("Setup failed for execution plan ${plan.id}: $message.")

/**
 * This exceptions is thrown whenever a single [ExecutionTask] fails.
 *
 * @param task The [ExecutionTask] that failed.
 * @param message An error message describing the circumstances.
 */
internal class TaskExecutionException(task: ExecutionTask, message: String) : Throwable("Execution failed for task ${task.id}: $message.")

/**
 * This exceptions is thrown whenever the setup of a single [ExecutionTask] fails. Errors of this kind are is usually caused
 * by mal-specification.
 *
 * @param task The [ExecutionTask] that failed.
 * @param message An error message describing the circumstances.
 */
internal class TaskSetupException(task: ExecutionTask, message: String) : Throwable("Setup failed for task ${task.id}: $message.")