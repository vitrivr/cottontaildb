package org.vitrivr.cottontail.dbms.general

import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An object that acts as unit of isolation for accesses (read/write) to the underlying [DBO].
 * [Tx] can be closed, committed and rolled back.
 *
 * [Tx] objects are bound to a specific [Transaction] which is a proxy for the global,
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

    /** [QueryContext] this [Tx] takes place in. */
    val context: QueryContext

    /**
     * A [Tx] that requires commit finalization, i.e., must execute actions before a commit can be executed.
     */
    interface WithCommitFinalization: Tx {
        /**
         * Called when the global transaction is rolled back. Can be used by this [Tx] to finalize its portion of the transaction.
         */
        fun beforeCommit()
    }

    /**
     * A [Tx] that requires rollback finalization, i.e., must execute actions before a commit can be executed.
     */
    interface WithRollbackFinalization: Tx {
        /**
         * Called when the global transaction is rolled back. Can be used by this [Tx] to finalize its portion of the transaction.
         */
        fun beforeRollback()
    }
}