package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.core.database.TransactionId

/**
 * An [Event] used in Cottontail DB's internal signalling infrastructure.
 *
 * [Event]s can be used to signal relevant changes to the system, schema or data
 * within and across transactions.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Event {
    /** The [TransactionId] this [Event] is associated with. */
    val txId: TransactionId
}