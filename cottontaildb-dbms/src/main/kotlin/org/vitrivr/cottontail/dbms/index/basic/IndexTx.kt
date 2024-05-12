package org.vitrivr.cottontail.dbms.index.basic

import org.vitrivr.cottontail.core.basics.Countable
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Filterable
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [SubTransaction] that operates on a single [Index]. [SubTransaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface IndexTx: SubTransaction, Filterable, Countable {
    /** Reference to the [Index] this [IndexTx] belongs to. */
    override val dbo: Index

    /** The parent [EntityTx] this [IndexTx] belongs to. */
    val parent: EntityTx

    /** The [QueryContext] this [IndexTx] belongs to. Typically determined by parent [EntityTx]. */
    val context: QueryContext
        get() = this.parent.context

    /** The [Transaction] this [IndexTx] belongs to. Typically determined by parent [EntityTx]. */
    override val transaction: Transaction
        get() = this.context.txn

    /** True, if the [Index] backing this [IndexTx] supports incremental updates, i.e., can be updated tuple by tuple. */
    val supportsIncrementalUpdate: Boolean

    /** True, if the [Index] backing this [IndexTx] supports asynchronous rebuilds, i.e., updates, and false otherwise. */
    val supportsAsyncRebuild: Boolean

    /** True, if the [Index] backing this [IndexTx] supports querying filtering a partition. */
    override val supportsPartitioning: Boolean

    /** The [IndexConfig] instance used by the [Index] backing this [IndexTx].*/
    val config: IndexConfig<*>

    /** The [ColumnDef]s indexed by the [Index] backing this [IndexTx]. */
    val columns: Array<ColumnDef<*>>

    /** The [IndexType] of the [Index] that underpins this [IndexTx]. */
    val state: IndexState

    /**
     * Calculates the cost estimate of this [IndexTx] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    fun costFor(predicate: Predicate): Cost

    /**
     * Returns the [ColumnDef] produced by this [IndexTx] for the given [Predicate].
     *
     * @param predicate [Predicate] to obtain columns for.
     * @return List of [ColumnDef] produced by this [IndexTx]
     */
    fun columnsFor(predicate: Predicate): List<ColumnDef<*>>

    /**
     * Returns the list of [Trait]s for the given [Predicate].
     *
     * @param predicate [Predicate] to obtain [Trait]s for.
     * @return List of [Trait]s produced by this [IndexTx]
     */
    fun traitsFor(predicate: Predicate): Map<TraitType<*>,Trait>

    /**
     * Inserts a new entry in the [Index] underlying this [IndexTx] based on the provided [DataEvent.Insert].
     *
     * Not all [Index] implementations support incremental updates. Should be indicated by [Index.supportsIncrementalUpdate]
     *
     * @param event [DataEvent.Insert] that should be processed.
     */
    fun insert(event: DataEvent.Insert)

    /**
     * Updates an entry in the [Index] underlying this [IndexTx] based on the provided [DataEvent.Update].
     *
     * Not all [Index] implementations support incremental updates. Should be indicated by [Index.supportsIncrementalUpdate]
     *
     * @param event [DataEvent.Update] that should be processed.
     */
    fun update(event: DataEvent.Update)

    /**
     * Deletes an entry from the [Index] underlying this [IndexTx] based on the provided [DataEvent.Delete].
     *
     * Not all [Index] implementations support incremental updates. Should be indicated by [Index.supportsIncrementalUpdate]
     *
     * @param event [DataEvent.Delete] that should be processed.
     */
    fun delete(event: DataEvent.Delete)

    /**
     * Performs a lookup through this [IndexTx] and returns a [Cursor] of all the [Tuple]s that match the [Predicate].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @return The resulting [Cursor].
     */
    override fun filter(predicate: Predicate): Cursor<Tuple>

    /**
     * Performs a lookup through this [IndexTx] and returns a [Cursor] of all the [Tuple]s that match the [Predicate]
     * and fall within the specified data [LongRange], which must lie in 0..[count].
     *
     * Not all [Index] implementations support range filtering.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
     * @return The resulting [Cursor].
     */
    override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple>
}