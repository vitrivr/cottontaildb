package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.Index

/**
 * A [IndexRebuilder] is a helper class that can be used to rebuild [Index] structures as part of a [TransactionContext].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface IndexRebuilder<T: Index> {
    /** The [Index] that this [IndexRebuilder] can rebuild. */
    val index: T

    /** The [TransactionContext] to rebuild the index with. */
    val context: TransactionContext

    /**
     * Starts the index rebuilding process for this [IndexRebuilder].
     *
     * @return True on success, false otherwise.
     */
    fun rebuild(): Boolean
}