package org.vitrivr.cottontail.dbms.column

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.general.Tx
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * A [Tx] that operates on a single [Column]. [Tx]s are a unit of isolation for data operations (read/write).
 *
 * This interface defines the basic operations supported by such a [Tx]. However, it does not
 * dictate the isolation level. It is up to the implementation to define and implement the desired
 * level of isolation.
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface ColumnTx<T : Value> : Tx {
    /** Reference to the [Column] this [ColumnTx] belongs to. */
    override val dbo: Column<T>

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