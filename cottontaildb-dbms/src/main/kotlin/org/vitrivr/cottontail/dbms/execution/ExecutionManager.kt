package org.vitrivr.cottontail.dbms.execution

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.vitrivr.cottontail.config.Config
import java.lang.Math.floorDiv
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * The [ExecutionManager] bu
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ExecutionManager(config: Config) {

    /** Internal counter to generate [Thread] IDs. Starts with 1 */
    private val queryWorkerCounter = AtomicInteger(1)

    /** Internal counter to generate [Thread] IDs. Starts with 1 */
    private val connectionWorkerCounter = AtomicInteger(1)

    /** Internal counter to generate [Thread] IDs. Starts with 1 */
    private val serviceWorkerCounter = AtomicInteger(1)

    /** The thread pool used by this [ExecutionManager] for query execution. */
    private val queryWorkerPool = ThreadPoolExecutor(config.execution.coreThreads, config.execution.maxThreads, config.execution.keepAliveMs, TimeUnit.MILLISECONDS, ArrayBlockingQueue(config.execution.queueSize), ThreadFactory {
        val thread = Thread(it, "cottontaildb-query-worker-${this.queryWorkerCounter.getAndIncrement()}")
        thread.priority = Thread.MAX_PRIORITY
        thread
    })

    /** The thread pool used by this [ExecutionManager] for the execution of service tasks. */
    internal val connectionWorkerPool = Executors.newFixedThreadPool(config.server.connectionThreads) {
        val thread = Thread(it, "cottontaildb-connection-handler-${this.connectionWorkerCounter.getAndIncrement()}")
        thread.priority = Thread.MAX_PRIORITY
        thread
    }

    /** The thread pool used by this [ExecutionManager] for the execution of service tasks. */
    private val serviceWorkerPool = Executors.newScheduledThreadPool(2) {
        val thread = Thread(it, "cottontaildb-service-worker-${this.serviceWorkerCounter.getAndIncrement()}")
        thread.priority = Thread.MIN_PRIORITY
        thread
    }

    /** [CoroutineDispatcher] for the query execution thread pool. */
    val queryDispatcher: CoroutineDispatcher = this.queryWorkerPool.asCoroutineDispatcher()

    /**
     * Returns the number of available worker threads.
     *
     * This is an estimate based on the state of the thread pool.
     *
     * @return Available worker threads.
     */
    fun availableQueryWorkers(): Int = this.queryWorkerPool.maximumPoolSize - this.queryWorkerPool.activeCount

    /**
     * Returns the number of workers available for intra query parallelism. T
     *
     * This is an estimate based on the state of the thread pool.
     *
     * @return Available worker threads.
     */
    fun availableIntraQueryWorkers(): Int = floorDiv(this.queryWorkerPool.maximumPoolSize, 2) - this.queryWorkerPool.activeCount

    /**
     * Shuts downs this [ExecutionManager].
     */
    fun shutdownAndWait() {
        this.serviceWorkerPool.shutdown()
        this.connectionWorkerPool.shutdown()
        this.queryWorkerPool.shutdown()
        this.serviceWorkerPool.awaitTermination(5000, TimeUnit.MILLISECONDS) &&
        this.queryWorkerPool.awaitTermination(5000, TimeUnit.MILLISECONDS) &&
        this.connectionWorkerPool.awaitTermination(5000, TimeUnit.MILLISECONDS)
    }
}