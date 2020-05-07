package org.vitrivr.cottontail.execution

import org.vitrivr.cottontail.config.ExecutionConfig
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


/**
 * The default [ExecutionEngine] for CottontailDB. It hosts all the necessary facilities to create and execute query [ExecutionPlan]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ExecutionEngine(config: ExecutionConfig) {

    /** The [ThreadPoolExecutor] used for executing queries. */
    private val executor = ThreadPoolExecutor(config.coreThreads, config.maxThreads, config.keepAliveMs, TimeUnit.MILLISECONDS, ArrayBlockingQueue(config.queueSize))

    /** The maximum amount of parallelism allowed by this [ExecutionEngine] instance. */
    val maxThreads
        get() = this.executor.maximumPoolSize

    /** The number of [Thread]s currently available. This is an estimate and may change very quickly. */
    val availableThreads
        get() = this.executor.maximumPoolSize - this.executor.activeCount

    /**
     * Creates and returns a new [ExecutionPlan].
     *
     * @return [ExecutionPlan]
     */
    fun newExecutionPlan() = ExecutionPlan(this.executor)
}