package org.vitrivr.cottontail.dbms.general

import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An object that acts as unit of isolation for accesses (read/write) to the underlying [DBO].
 * [Tx] can be closed, committed and rolled back.
 *
 * [Tx] objects are bound to a specific [TransactionContext] which is a proxy for the global,
 * ongoing transaction and which manages shared resources (e.g. multiple, concurrent [Tx] objects)
 * and locks to the [DBO]s.
 *
 * This interface defines the basic operations supported by a [Tx]. However, it does not dictate
 * the isolation level. It is up to the implementation to define and implement the desired
 * level of isolation and to obtain the necessary locks to safely execute an operation.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Tx {
    /** The [DBO] this [Tx] belongs to. */
    val dbo: DBO

    /** [TransactionContext] this [Tx] takes place in. */
    val context: TransactionContext

    /**
     * Called before the global transaction is committed.
     *
     * Can be used by this [Tx] to finalize its portion of the transaction.
     */
    fun beforeCommit()

    /**
     * Called when the global transaction is rolled back.
     *
     * Can be used by this [Tx] to finalize its portion of the transaction.
     */
    fun beforeRollback()
}