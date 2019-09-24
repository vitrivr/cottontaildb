package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.config.ExecutionConfig

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

    /**
     * Creates and returns a new [ExecutionPlan].
     *
     * @return [ExecutionPlan]
     */
    fun newExecutionPlan() = ExecutionPlan(this.executor)
}