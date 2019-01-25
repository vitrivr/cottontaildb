package ch.unibas.dmi.dbis.cottontail.database.general

import java.lang.Exception
import java.util.*


/**
 * This is a [Transaction] object as used by different [DBO] objects to facilitate changes.
 *
 * @author Ralph Gasser
 * @version 1.0f
 */
interface Transaction : AutoCloseable {
    /** [TransactionStatus] of this [Transaction]. */
    val status: TransactionStatus

    /** Indicator whether this [Transaction] is readonly. */
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

/**
 * An inline function that can be used to create a transactional context from a [Transaction].
 *
 * The provided block will be executed as a [Transaction] and any exception thrown in the block will result
 * in a rollback. Once the block has been executed successfully, the [Transaction] is committed.
 *
 * In both cases, the [Transaction] that has been used will be closed.
 *
 * @param block The block that should be executed in a [Transaction] context.
 */
fun <T:Transaction> T.begin(block: (tx: T) -> Boolean) = try {
    if (block(this)) {
        commit()
    } else {
        rollback()
    }
} catch (e: Exception) {
    rollback()
} finally {
    close()
}
