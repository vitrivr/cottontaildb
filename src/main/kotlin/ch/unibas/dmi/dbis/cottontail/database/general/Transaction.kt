package ch.unibas.dmi.dbis.cottontail.database.general

import java.util.*


/**
 * This is a [Transaction] object as used by different [DBO] objects to facilitate changes.
 */
interface Transaction : AutoCloseable {
    /** [TransactionStatus] of this [Transaction]. */
    val status: TransactionStatus

    /** Inidactor whether this [Transaction] is readonly. */
    val readonly: Boolean

    /** The UUID that identifies this [Transaction]. */
    val tid: UUID

    /**
     * Commits the transaction, saving all changes. Causes the [Transaction] to complete and close.
     */
    fun commit()

    /**
     * Rolls the [Transaction], causing changes to be lost. Causes the [Transaction] to complete and close.
     */
    fun rollback()
}