package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.max

/**
 * Config for Cottontail DB's task execution engine.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
@Serializable
data class ExecutionConfig(
    val coreThreads: Int = (Runtime.getRuntime().availableProcessors() / 2),
    val maxThreads: Int = 25,
    val keepAliveMs: Long = 1000L,
    val queueSize: Int = 100,
    val simd: Boolean = false,
    val transactionTableSize: Int = 100,
    val transactionHistorySize: Int = 500
) {

    /**
     *  Creates and returns a new [ThreadPoolExecutor] using this [ExecutionConfig].
     *
     *  @return [ThreadPoolExecutor]
     */
    fun newExecutor() = ThreadPoolExecutor(
        this.coreThreads.coerceAtLeast(1),
        this.maxThreads.coerceAtLeast(max(4, this.coreThreads)),
        this.keepAliveMs,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(this.queueSize)
    )
}