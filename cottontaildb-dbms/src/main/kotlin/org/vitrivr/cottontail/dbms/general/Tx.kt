package org.vitrivr.cottontail.dbms.general

import org.vitrivr.cottontail.dbms.execution.transactions.Transaction

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

    /** [Transaction] this [Tx] takes place in. */
    val transaction: Transaction

    /**
     * A [Tx] object, that performs actions prior to being committed.
     */
    interface BeforeCommit: Tx {
        fun beforeCommit()
    }

    /**
     * A [Tx] object, that performs actions prior to being rolled back.
     */
    interface BeforeRollback: Tx {
        fun beforeRollback()
    }

    /**
     * A [Tx] object, that performs some actions after to being committed.
     */
    interface AfterCommit: Tx {
        fun afterCommit()
    }

    /**
     * A [Tx] object, that performs some actions after to being rolled back.
     */
    interface AfterRollback: Tx {
        fun afterRollback()
    }

    /**
     * A [Tx] object, that can be committed or rolled back as part of a larger [Transaction]-
     */
    interface Commitable: Tx {
        /**
         * Commits this [Tx] and thus finalizes and persists all operations executed so far.
         */
        fun commit()

        /**
         * Aborts this [Tx] and rollsback all changes.
         */
        fun rollback()
    }
}