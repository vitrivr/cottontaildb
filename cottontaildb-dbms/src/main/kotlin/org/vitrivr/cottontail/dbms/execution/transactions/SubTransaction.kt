package org.vitrivr.cottontail.dbms.execution.transactions

import org.vitrivr.cottontail.dbms.general.DBO

/**
 * An object that acts as unit of isolation for accesses (read/write) to the underlying [DBO].
 * [SubTransaction] can be closed, committed and rolled back.
 *
 * [SubTransaction] objects are bound to a specific [Transaction] which is a proxy for the global,
 * ongoing transaction and which manages shared resources (e.g. multiple, concurrent [SubTransaction] objects)
 * and locks to the [DBO]s.
 *
 * This interface defines the basic operations supported by a [SubTransaction]. However, it does not dictate
 * the isolation level. It is up to the implementation to define and implement the desired
 * level of isolation and to obtain the necessary locks to safely execute an operation.
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface SubTransaction {
    /** The [DBO] this [SubTransaction] belongs to. */
    val dbo: DBO

    /** [Transaction] this [SubTransaction] takes place in. */
    val transaction: Transaction

    /**
     * A [SubTransaction] that has commit logic to execute.
     */
    interface WithCommit: SubTransaction {
        /**
         * Commits this [SubTransaction] and finalizes the changes made to the [DBO].
         */
        fun commit()

        /**
         * Called when the global transaction is about to commit. This method has two purposes:
         *
         * 1) It can be used by this [SubTransaction] to finalize its portion of the transaction.
         * 2) It is used to query a [SubTransaction]'s ability to commit its part of the global [Transaction].
         *
         * @return True, if this [SubTransaction] is ready to commit. False otherwise.
         */
        fun prepareCommit(): Boolean
    }

    /**
     * A [SubTransaction] that has explicit abort logic to execute.
     */
    interface WithAbort: SubTransaction {
        /**
         * Aborts this [SubTransaction] and reverts the changes made to the [DBO].
         */
        fun abort()
    }

    /**
     * A [SubTransaction] that requires rollback finalization, i.e., must execute actions before a commit can be executed.
     */
    interface WithFinalization: SubTransaction {
        /**
         * Called after the global transaction is committed. Can be used by this [SubTransaction] to finalize its portion of the transaction.
         *
         * This method must *not* make changes to the persistent state of the [DBO]!
         */
        fun finalize(commit: Boolean) {
            /* No op */
        }
    }
}