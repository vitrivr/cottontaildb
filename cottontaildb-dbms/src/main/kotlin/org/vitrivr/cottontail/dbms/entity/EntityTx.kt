package org.vitrivr.cottontail.dbms.entity

import org.vitrivr.cottontail.core.basics.Countable
import org.vitrivr.cottontail.core.basics.Modifiable
import org.vitrivr.cottontail.core.basics.Scanable
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * A [SubTransaction] that operates on a single [Entity]. [SubTransaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface EntityTx : SubTransaction, Scanable, Countable, Modifiable {
    /** The parent [SchemaTx] this [EntityTx] belongs to. */
    val parent: SchemaTx

    /** The [QueryContext] this [EntityTx] belongs to. Typically determined by parent [SchemaTx]. */
    val context: QueryContext
        get() = this.parent.context

    /** The [Transaction] this [EntityTx] belongs to. Typically determined by parent [SchemaTx]. */
    override val transaction: Transaction
        get() = this.context.txn

    /** Reference to the [Entity] this [EntityTx] belongs to. */
    override val dbo: Entity

    /**
     * Gets and returns [ValueStatistics] for the specified [ColumnDef].
     *
     * @return [ValueStatistics].
     */
    fun statistics(column: ColumnDef<*>): ValueStatistics<*>

    /**
     * Returns the smallest [TupleId] managed by the [Entity] backing this [EntityTx].
     *
     * @return The smallest [TupleId] in the [Entity] backing this [EntityTx]
     */
    fun smallestTupleId(): TupleId

    /**
     * Returns the largest [TupleId] managed by the [Entity] backing this [EntityTx].
     *
     * @return The largest [TupleId] in the [Entity] backing this [EntityTx]
     */
    fun largestTupleId(): TupleId

    /**
     * Returns true if the [Entity] underpinning this [EntityTx]contains the given [TupleId] and false otherwise.
     *
     * If this method returns true, then [EntityTx.read] will return a [Tuple] for [TupleId]. However, if this method
     * returns false, then [EntityTx.read] will throw an exception for that [TupleId].
     *
     * @param tupleId The [TupleId] of the desired entry
     * @return True if entry exists, false otherwise,
     */
    fun contains(tupleId: TupleId): Boolean

    /**
     * Reads the specified [TupleId] and the specified [ColumnDef] through this [EntityTx].
     *
     * @param tupleId The [TupleId] to read.
     */
    fun read(tupleId: TupleId): Tuple

    /**
     * Lists all [ColumnDef]s for the [Entity] associated with this [EntityTx].
     *
     * @return List of all [ColumnDef]s.
     */
    fun listColumns(): List<ColumnDef<*>>

    /**
     * Returns the [ColumnDef] for the specified [Name.ColumnName].
     *
     * @param name The [Name.ColumnName] of the column.
     * @return [ColumnDef] of the column.
     */
    fun columnForName(name: Name.ColumnName): ColumnDef<*>

    /**
     * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
     *
     * @return List of [Name.IndexName] managed by this [EntityTx]
     */
    fun listIndexes(): List<Name.IndexName>

    /**
     * Returns the [IndexTx] for the given [Name.IndexName].
     *
     * @param name The [Name.IndexName] of the [Index] the [IndexTx] belongs to.
     * @return [IndexTx]
     */
    fun indexForName(name: Name.IndexName): Index

    /**
     * Creates the [Index] with the given settings
     *
     * @param name [Name.IndexName] of the [Index] to create.
     * @param type Type of the [Index] to create.
     * @param columns The list of [Name.ColumnName] to create [Index] for.
     * @param configuration The [IndexConfig] to initialize the [Index] with.
     * @return Newly created [Index] for use in context of this [SubTransaction]
     */
    fun createIndex(name: Name.IndexName, type: IndexType, columns: List<Name.ColumnName>, configuration: IndexConfig<*>): Index

    /**
     * Drops the [Index] with the given name.
     *
     * @param name [Name.IndexName] of the [Index] to drop.
     */
    fun dropIndex(name: Name.IndexName)

    /**
     * Truncates the [Entity] backed by this [EntityTx]. This operation will remove all data from the [Entity
     */
    fun truncate()
}