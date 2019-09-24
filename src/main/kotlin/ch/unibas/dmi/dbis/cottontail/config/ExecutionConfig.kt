package ch.unibas.dmi.dbis.cottontail.config

import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Shape
import kotlinx.serialization.Serializable

/**
 * Config for Cottontail DB's task execution engine.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
@Serializable
data class ExecutionConfig(
    val coreThreads: Int = (Runtime.getRuntime().availableProcessors()/2),
    val maxThreads: Int = Runtime.getRuntime().availableProcessors(),
    val vectorization: Shape = Shape.OFF,
    val keepAliveMs: Long = 1000L,
    val queueSize: Int = 100
)