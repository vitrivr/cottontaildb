package org.vitrivr.cottontail.dbms.execution.transactions

import org.vitrivr.cottontail.core.database.TransactionId

/**
 * A proxy for a finished transaction.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class FinishedTransaction(
    override val transactionId: TransactionId,
    override val type: TransactionType,
    override val state: TransactionStatus,
    override val created: Long,
    override val ended: Long,
    override val numberOfSuccess: Int,
    override val numberOfError: Int,
    override val numberOfOngoing: Int
): TransactionMetadata  {
    constructor(tx: Transaction): this(tx.transactionId, tx.type, tx.state, tx.created, tx.ended ?: System.currentTimeMillis(), tx.numberOfSuccess, tx.numberOfError, tx.numberOfOngoing)
}