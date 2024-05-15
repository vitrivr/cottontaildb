package org.vitrivr.cottontail.dbms.sequence

import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * A [SubTransaction] that operates on a single [SequenceTx].
 *
 * [SubTransaction]s are a unit of isolation for data  operations (read/write).
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface SequenceTx: SubTransaction {
    /** The parent [SchemaTx] this [SequenceTx] belongs to. */
    val parent: SchemaTx

    /** The [QueryContext] this [SequenceTx] belongs to. Typically determined by parent [SchemaTx]. */
    val context: QueryContext
        get() = this.parent.context

    /** The [Transaction] this [SequenceTx] belongs to. Typically determined by parent [SchemaTx]. */
    override val transaction: Transaction
        get() = this.context.txn

    /**
     * Returns the next value of this [Sequence].
     *
     * @return The next [LongValue] value in the [Sequence].
     */
    fun next(): LongValue

    /**
     * Returns the current value of this [Sequence] without changing it.
     *
     * @return The next [LongValue] value in the [Sequence].
     */
    fun current(): LongValue

    /**
     * Resets this [Sequence], without changing it.
     *
     * @return The next [LongValue] value in the [Sequence].
     */
    fun reset(): LongValue
}