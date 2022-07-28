package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import java.io.Closeable

/**
 * A [AsyncIndexRebuilder] is a helper class that can be used to rebuild [Index] over multiple [TransactionContext] s part
 * of Cottontail DB's adaptive index management framework.
 *
 * The [AsyncIndexRebuilder] de-couples the step uf building-up and merging the changes with the actual [Index] structure.
 * This can be advantageous for [Index] structures, that require a long time to rebuild. The first (long) step can be
 * executed in a read-only [TransactionContext], using non-blocking reads while the second (shorter) step is executed
 * in a separate [TransactionContext] thereafter.
 *
 * In order to be informed about changes that happen in the meanwhile, the [AsyncIndexRebuilder] implements the
 * [TransactionObserver], which it uses to be informed about changes to the data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface AsyncIndexRebuilder <T: Index>: TransactionObserver, Closeable {
    /** The [Index] that this [AsyncIndexRebuilder] can rebuild. */
    val index: T

    /** The current state of this [AsyncIndexRebuilder]. */
    val state: IndexRebuilderState

    /**
     * Scans the data necessary for this [AsyncIndexRebuilder]. Usually, this takes place within an existing [QueryContext].
     *
     * @param context1 The [QueryContext] to perform the MERGE in.
     */
    fun scan(context1: QueryContext)

    /**
     * Merges this [AbstractAsyncIndexRebuilder] with its [IndexTx] using the given [QueryContext].
     *
     * @param context2 The [QueryContext] to perform the MERGE in.
     */
    fun merge(context2: QueryContext)
}