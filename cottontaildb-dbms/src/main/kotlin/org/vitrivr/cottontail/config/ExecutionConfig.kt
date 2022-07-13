package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable

/**
 * Configuration for Cottontail DB's query execution engine.
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
@Serializable
data class ExecutionConfig(
    val coreThreads: Int = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1),
    val maxThreads: Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(1) * 2,
    val keepAliveMs: Long = 10000L,
    val queueSize: Int = 100,
    val simd: Boolean = false,
    val transactionTableSize: Int = 100,
    val transactionHistorySize: Int = 500
)