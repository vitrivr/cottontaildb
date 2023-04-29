package org.vitrivr.cottontail.ui.model.system

/**
 * A [Transaction] as returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Transaction(
    val txId: Long,
    val type: String,
    val state: TransactionStatus,
    val created: String,
    val ended: String?,
    val duration: Double
)