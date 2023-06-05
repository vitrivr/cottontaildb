package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class MemoryConfig(
    /** The maximum amount of memory a sort operation can use to perform sorting in memory. */
    val maxSortBufferSize: Long = 100_000_000L
)