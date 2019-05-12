package ch.unibas.dmi.dbis.cottontail.config

import kotlinx.serialization.Optional
import kotlinx.serialization.Serializable

/**
 * Config for Cottontail DB's [ExecutionEngine]. Its values determine, how many threads can handle queries and how many
 * queries can be enqueued, before queries are being rejected.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@Serializable
data class ExecutionConfig(
    val coreThreads: Int = (Runtime.getRuntime().availableProcessors()/2),
    val maxThreads: Int = Runtime.getRuntime().availableProcessors(),
    val keepAliveMs: Long = 1000L,
    val queueSize: Int = 100
)