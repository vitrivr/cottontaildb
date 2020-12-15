package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.model.basics.*

/**
 * A [Tx] that operates on a single [Entity]. [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface EntityTx : Tx, Scanable, Countable, Modifiable {

    /** Reference to the [Entity] this [EntityTx] belongs to. */
    override val dbo: Entity

    /**
     * Returns the maximum [TupleId] known by this [EntityTx].
     *
     * @return Maximum [TupleId] known by this [EntityTx]
     */
    fun maxTupleId(): TupleId

    /**
     * Lists all [ColumnDef]s for the [Entity] associated with this [EntityTx].
     *
     * @return List of all [ColumnDef]s.
     */
    fun listColumns(): List<Column<*>>

    /**
     * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
     *
     * @return List of [Name.IndexName] managed by this [EntityTx]
     */
    fun listIndexes(): List<Index>

    /**
     * Returns the [Column] for the specified [Name.ColumnName]. Should be able to handle
     * both simple names as well as fully qualified names.
     *
     * @param name The [Name.ColumnName] of the [Column].
     * @return [Column].
     */
    fun columnForName(name: Name.ColumnName): Column<*>

    /**
     * Returns the [IndexTx] for the given [Name.IndexName].
     *
     * @param name The [Name.IndexName] of the [Index] the [IndexTx] belongs to.
     * @return [IndexTx]
     */
    fun indexForName(name: Name.IndexName): Index

    /**
     *
     */
    fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record
}