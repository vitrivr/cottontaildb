package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.math.knn.metrics.Shape

/**
 * Config for Cottontail DB's task execution engine.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
@Serializable
data class ExecutionConfig(
        val coreThreads: Int = (Runtime.getRuntime().availableProcessors() / 2),
        val maxThreads: Int = Runtime.getRuntime().availableProcessors(),
        val vectorization: Shape = Shape.OFF,
        val keepAliveMs: Long = 1000L,
        val queueSize: Int = 100
)