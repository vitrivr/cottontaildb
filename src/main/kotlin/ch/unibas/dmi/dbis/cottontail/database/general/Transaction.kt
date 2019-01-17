package ch.unibas.dmi.dbis.cottontail.database.general

/**
 *
 */
interface Transaction : AutoCloseable {
    /** [AccessorMode] of this [Transaction]. */
    val mode : AccessorMode

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
    /**
     * Returns true, if this [Transaction] is read only read only, and false otherwise.
     *
     * @return true, if this [Transaction], and false otherwise.
     */
    fun readOnly(): Boolean = (mode === AccessorMode.READONLY)
}