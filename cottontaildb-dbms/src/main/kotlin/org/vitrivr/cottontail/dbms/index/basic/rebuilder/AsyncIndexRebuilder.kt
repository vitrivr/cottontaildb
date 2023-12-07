package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import java.io.Closeable

/**
 * A [AsyncIndexRebuilder] is a helper class that can be used to rebuild [Index] over multiple [Transaction] s part
 * of Cottontail DB's adaptive index management framework.
 *
 * The [AsyncIndexRebuilder] de-couples the step uf building-up and merging the changes with the actual [Index] structure.
 * This can be advantageous for [Index] structures, that require a long time to rebuild. The first (long) step can be
 * executed in a read-only [Transaction], using non-blocking reads while the second (shorter) step is executed
 * in a separate [Transaction] thereafter.
 *
 * In order to be informed about changes that happen in the meanwhile, the [AsyncIndexRebuilder] implements the
 * [TransactionObserver], which it uses to be informed about changes to the data.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface AsyncIndexRebuilder <T: Index>: TransactionObserver, Closeable {
    /** The [Index] that this [AsyncIndexRebuilder] can rebuild. */
    val index: T

    /** The current state of this [AsyncIndexRebuilder]. */
    val state: IndexRebuilderState

    /**
     * Scans the data necessary for this [AsyncIndexRebuilder]. Typically uses a read-only [QueryContext].
     */
    fun build()

    /**
     * Replaces this [AbstractAsyncIndexRebuilder]. Requires uses an exclusive [QueryContext].
     */
    fun replace()
}