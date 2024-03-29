package org.vitrivr.cottontail.ui.model.system

import kotlinx.serialization.Serializable

/**
 * A [Transaction] as returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class Transaction(
    val txId: Long,
    val type: String,
    val state: TransactionStatus,
    val created: String,
    val ended: String?,
    val duration: Double
)