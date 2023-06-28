package org.vitrivr.cottontail.dbms.execution.transactions

import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext

/**
 * A [TransactionMetadata] that can be used to execute [Operator]s in a given [DefaultQueryContext].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface TransactionMetadata {

    /** The [TransactionId] of this [TransactionMetadata]. */
    val transactionId: TransactionId

    /** The [TransactionType] of this [TransactionMetadata]. */
    val type: TransactionType

    /** The [TransactionStatus] of this [TransactionMetadata]. */
    val state: TransactionStatus

    /** The timestamp at which this [TransactionMetadata] was created. */
    val created: Long

    /** The timestamp at which this [TransactionMetadata] has ended. May be null if it is still ongoing. */
    val ended: Long?

    /** Number of queries executed successfully in this [TransactionMetadata]. */
    val numberOfSuccess: Int

    /** Number of queries executed with an error in this [TransactionMetadata]. */
    val numberOfError: Int

    /** Number of ongoing queries in this [TransactionMetadata]. */
    val numberOfOngoing: Int
}