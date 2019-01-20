package ch.unibas.dmi.dbis.cottontail.database.general

/**
 *
 */
interface Transaction : AutoCloseable {
    /** TransactionStatus] of this [Transaction]. */
    val status: TransactionStatus

    /**
     * Executes the provided action in a [Transaction] context. If the action returns true and no
     * exceptions occur, the [Transaction] will commit immediately. Otherwise the [Transaction] will rollback.
     */
    fun execute(action: () -> Boolean) = try {
        if (action()) {
            commit()
        } else {
            rollback()
        }
    } catch (e: Exception) {
        this.rollback()
    }

    /**
     * Commits the transaction, saving all changes. Causes the [Transaction] to complete and close.
     */
    fun commit()

    /**
     * Rolls the [Transaction], causing changes to be lost. Causes the [Transaction] to complete and close.
     */
    fun rollback()
}