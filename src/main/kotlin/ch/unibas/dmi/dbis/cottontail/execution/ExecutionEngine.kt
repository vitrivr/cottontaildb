package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.config.Config

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit



class ExecutionEngine(config: Config) {

    /** The [ThreadPoolExecutor] used for executing queries. */
    private val executor = ThreadPoolExecutor(config.executionConfig.coreThreads, config.executionConfig.maxThreads, config.executionConfig.keepAliveMs, TimeUnit.MILLISECONDS, ArrayBlockingQueue(config.executionConfig.queueSize))


    /**
     * Creates and returns a new [ExecutionPlan].
     */
    fun newExecutionPlan() = ExecutionPlan(this.executor)
}