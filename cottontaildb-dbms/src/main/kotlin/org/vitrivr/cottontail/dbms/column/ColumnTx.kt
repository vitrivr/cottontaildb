package org.vitrivr.cottontail.dbms.column

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * A [SubTransaction] that operates on a single [Column]. [SubTransaction]s are a unit of isolation for data operations (read/write).
 *
 * This interface defines the basic operations supported by such a [SubTransaction]. However, it does not
 * dictate the isolation level. It is up to the implementation to define and implement the desired
 * level of isolation.
 *
 * @author Ralph Gasser
 * @version 5.0.0
 */
interface ColumnTx<T : Value> : SubTransaction {
    /** Reference to the [Column] this [ColumnTx] belongs to. */
    override val dbo: Column<T>

    /** The parent [EntityTx] this [ColumnTx] belongs to. */
    val parent: EntityTx

    /** The [QueryContext] this [ColumnTx] belongs to. Typically determined by parent [EntityTx]. */
    val context: QueryContext
        get() = this.parent.context

    /** The [Transaction] this [ColumnTx] belongs to. Typically determined by parent [EntityTx]. */
    override val transaction: Transaction
        get() = this.context.txn

    /** The [ColumnDef] of the [Column] underlying this [ColumnTx]. */
    val columnDef: ColumnDef<T>
        get() = this.dbo.columnDef

    /**
     * Gets and returns [ValueStatistics] for the [Column] backing this [ColumnTx]
     *
     * @return [ValueStatistics].
     */
    fun statistics(): ValueStatistics<T>

    /**
     * Gets and returns an entry from this [Column].
     *
     * @param tupleId The [TupleId] of the desired entry
     * @return The desired entry.
     */
    fun read(tupleId: TupleId): T?

    /**
     * Updates the entry with the specified [TupleId] and sets it to the new [Value].
     *
     * @param tupleId The [TupleId] of the entry that should be updated.
     * @param value The new [Value]
     * @return The old [Value]
     */
    fun write(tupleId: TupleId, value: T): T?

    /**
     * Deletes the entry with the specified [TupleId] .
     *
     * @param tupleId The [TupleId] of the entry that should be deleted.
     * @return The old [Value]
     */
    fun delete(tupleId: TupleId): T?

    /**
     * Returns a [Cursor] for the [Column] underpinning this [ColumnTx].
     *
     * @return [Cursor]
     */
    fun cursor(): Cursor<T?>
}