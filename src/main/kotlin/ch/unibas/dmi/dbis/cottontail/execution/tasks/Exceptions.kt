package ch.unibas.dmi.dbis.cottontail.execution.tasks

import com.github.dexecutor.core.task.Task


class TaskExecutionException(task: Task<*,*>, message: String) : Throwable("Task ${task.id} failed to excute: $message.")

class TaskSpecificationException(task: Task<*,*>, message: String) : Throwable("Ill-specified task ${task.id}: $message.")