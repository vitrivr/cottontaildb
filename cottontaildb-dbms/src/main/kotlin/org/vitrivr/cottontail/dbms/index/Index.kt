package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder

/**
 * Represents a (secondary) [Index] structure in the Cottontail DB data model. An [Index] belongs
 * to an [Entity] and can be used to index one to many [Column]s. Usually, [Entity]es allow for
 * faster data access.
 *
 * [Index] structures are uniquely identified by their [Name.IndexName].
 *
 * @see IndexTx
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Index : DBO {

    /** [Entity] this [Index] belongs to. */
    override val parent: Entity

    /** The [Name.IndexName] of this [Index]. */
    override val name: Name.IndexName

    /** The [ColumnDef] that are covered (i.e. indexed) by this [Index]. */
    val columns: Array<ColumnDef<*>>

    /** The [ColumnDef] that are produced by this [Index]. They often differ from the indexed columns. */
    val produces: Array<ColumnDef<*>>

    /** The order in which results of this [Index] appear. Empty array that there is no particular order. */
    val order: Array<Pair<ColumnDef<*>, SortOrder>>

    /** True, if the [Index] supports incremental updates, and false otherwise. */
    val supportsIncrementalUpdate: Boolean

    /** True, if the [Index] supports querying filtering an indexable range of the data. */
    val supportsPartitioning: Boolean

    /** The type of [Index]. */
    val type: IndexType

    /** Flag indicating, if this [Index] reflects all changes done to the [Entity] it belongs to. */
    val state: IndexState

    /** The configuration map used for the [Index]. */
    val config: IndexConfig

    /** Number of entries in this [Index]. */
    val count: Long

    /**
     * Checks if this [Index] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    fun canProcess(predicate: Predicate): Boolean

    /**
     * Calculates the cost estimate if this [Index] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    fun cost(predicate: Predicate): Cost

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param context If the [TransactionContext] that requested the [IndexTx].
     */
    override fun newTx(context: TransactionContext): IndexTx
}