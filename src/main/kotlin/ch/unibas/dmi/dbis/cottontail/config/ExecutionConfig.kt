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
    @Optional val coreThreads: Int = (Runtime.getRuntime().availableProcessors()/2),
    @Optional val maxThreads: Int = Runtime.getRuntime().availableProcessors(),
    @Optional val keepAliveMs: Long = 1000L,
    @Optional val queueSize: Int = 100
)