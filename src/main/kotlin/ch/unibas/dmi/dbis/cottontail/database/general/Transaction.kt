package ch.unibas.dmi.dbis.cottontail.database.general

/**
 *
 */
interface Transaction : AutoCloseable {
    /** TransactionStatus] of this [Transaction]. */
    val status: TransactionStatus

    /**
     * Commits the transaction, saving all changes. Causes the [Transaction] to complete and close.
     */
    fun commit()

    /**
     * Rolls the [Transaction], causing changes to be lost. Causes the [Transaction] to complete and close.
     */
    fun rollback()
}