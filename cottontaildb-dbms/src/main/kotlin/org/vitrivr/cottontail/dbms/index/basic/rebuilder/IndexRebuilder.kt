package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [IndexRebuilder] is a helper class that can be used to rebuild [Index] structures as part of a [Transaction].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface IndexRebuilder<T: Index> {
    /** The [Index] that this [IndexRebuilder] can rebuild. */
    val index: T

    /** The [QueryContext] to rebuild the index with. */
    val context: QueryContext

    /**
     * Starts the index rebuilding process for this [IndexRebuilder].
     *
     * @param indexTx The [IndexTx] to use.
     * @return True on success, false otherwise.
     */
    fun rebuild(indexTx: IndexTx): Boolean
}